/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.spring.beans.factory.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.BridgeMethodResolver;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.convert.ConversionService;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.common.TemplateAwareExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import reactor.core.Observable;
import reactor.event.Event;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.spring.annotation.ReplyTo;
import reactor.spring.annotation.Selector;
import reactor.util.StringUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import static reactor.event.selector.Selectors.*;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ConsumerBeanPostProcessor implements BeanPostProcessor,
		BeanFactoryAware,
		Ordered {

	private static final Logger                       LOG                    = LoggerFactory.getLogger(
			ConsumerBeanPostProcessor.class);
	public static final ReflectionUtils.MethodFilter CONSUMER_METHOD_FILTER = new ReflectionUtils.MethodFilter() {
		@Override
		public boolean matches(Method method) {
			return null != AnnotationUtils.findAnnotation(method, Selector.class);
		}
	};

	private BeanResolver beanResolver;
	private TemplateAwareExpressionParser expressionParser = new SpelExpressionParser();
	private final ConversionService conversionService;

	public ConsumerBeanPostProcessor() {
		this.conversionService = new DefaultFormattingConversionService();
	}

	@Autowired(required = false)
	public ConsumerBeanPostProcessor(ConversionService conversionService) {
		this.conversionService = conversionService;
	}

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		beanResolver = new BeanFactoryResolver(beanFactory);
	}

	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE;
	}

	@Override
	public Object postProcessBeforeInitialization(Object bean,
	                                              String beanName) throws BeansException {
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(final Object bean,
	                                             String beanName) throws BeansException {
		return initBean(bean, findHandlerMethods(bean.getClass(), CONSUMER_METHOD_FILTER));
	}

	public Object initBean(final Object bean, final Set<Method> methods) {
		if (methods == null || methods.isEmpty())
			return bean;

		Consumer consumer;
		Observable reactor;
		Selector selectorAnno;
		ReplyTo replyToAnno;
		reactor.event.selector.Selector selector;

		for (final Method method : methods) {
			//scanAnnotation method
			selectorAnno = AnnotationUtils.findAnnotation(method, Selector.class);
			replyToAnno = AnnotationUtils.findAnnotation(method, ReplyTo.class);
			reactor = fetchObservable(selectorAnno, bean);
			selector = fetchSelector(selectorAnno, bean, method);

			//register [replyTo]consumer
			Object replyTo = replyToAnno != null ? parseReplyTo(replyToAnno, bean) : null;
			Invoker handler = new Invoker(method, bean);
			consumer = null != replyToAnno ?
					new ReplyToServiceConsumer(reactor, replyTo, handler) :
					new ServiceConsumer(handler);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Attaching Consumer to Reactor[" + reactor + "] using Selector[" + selector + "]");
			}

			if (null == selector) {
				reactor.on(consumer);
			} else {
				reactor.on(selector, consumer);
			}
		}
		return bean;
	}

	@SuppressWarnings("unchecked")
	private <T> T expression(String selector, Object bean) {
		if (selector == null)
			return null;

		StandardEvaluationContext evalCtx = new StandardEvaluationContext();
		evalCtx.setRootObject(bean);
		evalCtx.setBeanResolver(beanResolver);

		return (T) expressionParser.parseExpression(selector).getValue(evalCtx);
	}

	protected Observable fetchObservable(Selector selectorAnno, Object bean) {
		return expression(selectorAnno.reactor(), bean);
	}

	protected Object parseSelector(Selector selector, Object bean, Method method) {
		if (!StringUtils.hasText(selector.value())) {
			return method.getName();
		}

		try {
			return expression(selector.value(), bean);
		} catch (EvaluationException ee) {
			return selector.value();
		}

	}

	protected Object parseReplyTo(ReplyTo selector, Object bean) {
		if (StringUtils.isEmpty(selector.value())) {
			return null;
		}
		try {
			return expression(selector.value(), bean);
		} catch (EvaluationException ee) {
			return selector.value();
		}
	}

	protected reactor.event.selector.Selector fetchSelector(Selector selectorAnno, Object bean, Method method) {
		Object sel = parseSelector(selectorAnno, bean, method);
		try {
			switch (selectorAnno.type()) {
				case OBJECT:
					return object(sel);
				case REGEX:
					return regex(sel.toString());
				case URI:
					return uri(sel.toString());
				case TYPE:
					try {
						return type(Class.forName(sel.toString()));
					} catch (ClassNotFoundException e) {
						throw new IllegalArgumentException(e.getMessage(), e);
					}
			}
		} catch (EvaluationException e) {
			if (LOG.isTraceEnabled()) {
				LOG.trace("Creating ObjectSelector for '" + sel + "' due to " + e.getMessage(), e);
			}
		}
		return object(sel);
	}

	protected final static class ReplyToServiceConsumer implements Consumer<Event> {

		final protected Observable reactor;
		final protected Object     replyToKey;
		final protected Invoker    handler;

		ReplyToServiceConsumer(Observable reactor, Object replyToKey, Invoker handler) {
			this.reactor = reactor;
			this.replyToKey = replyToKey;
			this.handler = handler;
		}

		@Override
		public void accept(Event ev) {
			Object result = handler.apply(ev);
			Object _replyToKey = replyToKey != null ? replyToKey : ev.getReplyTo();
			if (_replyToKey != null)
				reactor.notify(_replyToKey, Event.wrap(result));
		}
	}

	protected final static class ServiceConsumer implements Consumer<Event> {
		final protected Invoker handler;

		ServiceConsumer(Invoker handler) {
			this.handler = handler;
		}

		@Override
		public void accept(Event ev) {
			handler.apply(ev);
		}
	}

	protected final class Invoker implements Function<Event, Object> {

		final protected Method     method;
		final protected Object     bean;
		final protected Class<?>[] argTypes;

		Invoker(Method method, Object bean) {
			this.method = method;
			this.bean = bean;
			this.argTypes = method.getParameterTypes();
		}

		@Override
		public Object apply(Event ev) {
			if (argTypes.length == 0) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Invoking method[" + method + "] on " + bean + " using " + ev);
				}
				return ReflectionUtils.invokeMethod(method, bean);
			}

			if (argTypes.length > 1) {
				throw new IllegalStateException("Multiple parameters not yet supported.");
			}

			if (null == ev.getData() || argTypes[0].isAssignableFrom(ev.getData().getClass())) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Invoking method[" + method + "] on " + bean + " using " + ev.getData());
				}
				return ReflectionUtils.invokeMethod(method, bean, ev.getData());
			}

			if (!argTypes[0].isAssignableFrom(ev.getClass())
					&& conversionService.canConvert(ev.getClass(), argTypes[0])) {
				ReflectionUtils.invokeMethod(method, bean, conversionService.convert(ev, argTypes[0]));
			}

			if (conversionService.canConvert(ev.getData().getClass(), argTypes[0])) {
				Object convertedObj = conversionService.convert(ev.getData(), argTypes[0]);
				if (LOG.isDebugEnabled()) {
					LOG.debug("Invoking method[" + method + "] on " + bean + " using " + convertedObj);
				}
				return ReflectionUtils.invokeMethod(method, bean, convertedObj);
			}

			throw new IllegalArgumentException("Cannot invoke method " + method + " passing parameter " + ev.getData());
		}
	}

	public static Set<Method> findHandlerMethods(Class<?> handlerType,
	                                             final ReflectionUtils.MethodFilter handlerMethodFilter) {
		final Set<Method> handlerMethods = new LinkedHashSet<Method>();
		Set<Class<?>> handlerTypes = new LinkedHashSet<Class<?>>();
		Class<?> specificHandlerType = null;
		if (!Proxy.isProxyClass(handlerType)) {
			handlerTypes.add(handlerType);
			specificHandlerType = handlerType;
		}
		handlerTypes.addAll(Arrays.asList(handlerType.getInterfaces()));
		for (Class<?> currentHandlerType : handlerTypes) {
			final Class<?> targetClass = (specificHandlerType != null ? specificHandlerType : currentHandlerType);
			ReflectionUtils.doWithMethods(currentHandlerType, new ReflectionUtils.MethodCallback() {
				@Override
				public void doWith(Method method) {
					Method specificMethod = ClassUtils.getMostSpecificMethod(method, targetClass);
					Method bridgedMethod = BridgeMethodResolver.findBridgedMethod(specificMethod);
					if (handlerMethodFilter.matches(specificMethod) &&
							(bridgedMethod == specificMethod || !handlerMethodFilter.matches(bridgedMethod))) {
						handlerMethods.add(specificMethod);
					}
				}
			}, ReflectionUtils.USER_DECLARED_METHODS);
		}
		return handlerMethods;
	}

}
