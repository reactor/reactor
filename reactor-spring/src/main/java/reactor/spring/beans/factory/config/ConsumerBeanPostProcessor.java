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

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

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
import org.springframework.expression.Expression;
import org.springframework.expression.common.TemplateAwareExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import reactor.event.Event;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.core.Observable;
import reactor.spring.annotation.ReplyTo;
import reactor.spring.annotation.Selector;
import reactor.util.StringUtils;

/**
 * @author Jon Brisbin
 */
public class ConsumerBeanPostProcessor implements BeanPostProcessor,
                                                  BeanFactoryAware,
                                                  Ordered {

	private static final Logger                       LOG                    = LoggerFactory.getLogger(
			ConsumerBeanPostProcessor.class);
	private static final ReflectionUtils.MethodFilter CONSUMER_METHOD_FILTER = new ReflectionUtils.MethodFilter() {
		@Override public boolean matches(Method method) {
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

	@Override public int getOrder() {
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
		Class<?> type = bean.getClass();
		for(final Method method : findHandlerMethods(type, CONSUMER_METHOD_FILTER)) {
			StandardEvaluationContext evalCtx = new StandardEvaluationContext();
			evalCtx.setRootObject(bean);
			evalCtx.setBeanResolver(beanResolver);

			Selector selectorAnno = method.getAnnotation(Selector.class);
			ReplyTo replyToAnno = method.getAnnotation(ReplyTo.class);

			final Observable reactor = (Observable)expressionParser.parseExpression(selectorAnno.reactor()).getValue(evalCtx);
			Assert.notNull(reactor, "Reactor cannot be null");

			reactor.event.selector.Selector selector = null;
			if(StringUtils.hasText(selectorAnno.value())) {
				try {
					switch(selectorAnno.type()) {
						case OBJECT: {
							Expression selectorExpr = expressionParser.parseExpression(selectorAnno.value());
							Object selObj = selectorExpr.getValue(evalCtx);
							selector = Selectors.object(selObj);
							break;
						}
						case REGEX: {
							selector = Selectors.regex(selectorAnno.value());
							break;
						}
						case URI: {
							selector = Selectors.uri(selectorAnno.value());
							break;
						}
						case TYPE: {
							try {
								selector = Selectors.type(Class.forName(selectorAnno.value()));
							} catch(ClassNotFoundException e) {
								throw new IllegalArgumentException(e.getMessage(), e);
							}
							break;
						}
					}
				} catch(EvaluationException e) {
					if(LOG.isTraceEnabled()) {
						LOG.trace("Creating ObjectSelector for '" + selectorAnno.value() + "' due to " + e.getMessage(), e);
					}
					selector = Selectors.object(selectorAnno.value());
				}
			}

			final Function<Event<Object>, Object> handler = new Function<Event<Object>, Object>() {
				Class<?>[] argTypes = method.getParameterTypes();

				@Override
				public Object apply(Event<Object> ev) {
					if(argTypes.length == 0) {
						if(LOG.isDebugEnabled()) {
							LOG.debug("Invoking method[" + method + "] on " + bean + " using " + ev);
						}
						return ReflectionUtils.invokeMethod(method, bean);
					}

					if(argTypes.length > 1) {
						// TODO: handle more than one parameter
						throw new IllegalStateException("Multiple parameters not yet supported.");
					}

					if(null == ev.getData() || argTypes[0].isAssignableFrom(ev.getData().getClass())) {
						if(LOG.isDebugEnabled()) {
							LOG.debug("Invoking method[" + method + "] on " + bean + " using " + ev.getData());
						}
						return ReflectionUtils.invokeMethod(method, bean, ev.getData());
					}

					if(!argTypes[0].isAssignableFrom(ev.getClass())
							&& conversionService.canConvert(ev.getClass(), argTypes[0])) {
						ReflectionUtils.invokeMethod(method, bean, conversionService.convert(ev, argTypes[0]));
					}

					if(conversionService.canConvert(ev.getData().getClass(), argTypes[0])) {
						Object convertedObj = conversionService.convert(ev.getData(), argTypes[0]);
						if(LOG.isDebugEnabled()) {
							LOG.debug("Invoking method[" + method + "] on " + bean + " using " + convertedObj);
						}
						return ReflectionUtils.invokeMethod(method, bean, convertedObj);
					}

					throw new IllegalArgumentException("Cannot invoke method " + method + " passing parameter " + ev.getData());
				}
			};

			String replyTo = (null != replyToAnno ? replyToAnno.value() : null);
			Consumer<Event<Object>> consumer;
			if(StringUtils.hasText(replyTo)) {
				Expression replyToExpr = expressionParser.parseExpression(replyTo);
				Object replyToObj;
				try {
					replyToObj = replyToExpr.getValue(evalCtx);
				} catch(EvaluationException ignored) {
					replyToObj = replyTo;
				}

				final Object replyToKey = replyToObj;
				consumer = new Consumer<Event<Object>>() {
					@Override public void accept(Event<Object> ev) {
						Object result = handler.apply(ev);
						reactor.notify(replyToKey, Event.wrap(result));
					}
				};
			} else {
				consumer = new Consumer<Event<Object>>() {
					@Override public void accept(Event<Object> ev) {
						handler.apply(ev);
					}
				};
			}

			if(LOG.isDebugEnabled()) {
				LOG.debug("Attaching Consumer to Reactor[" + reactor + "] using Selector[" + selector + "]");
			}

			if(null == selector) {
				reactor.on(consumer);
			} else {
				reactor.on(selector, consumer);
			}
		}
		return bean;
	}

	public static Set<Method> findHandlerMethods(Class<?> handlerType,
	                                       final ReflectionUtils.MethodFilter handlerMethodFilter) {
		final Set<Method> handlerMethods = new LinkedHashSet<Method>();
		Set<Class<?>> handlerTypes = new LinkedHashSet<Class<?>>();
		Class<?> specificHandlerType = null;
		if(!Proxy.isProxyClass(handlerType)) {
			handlerTypes.add(handlerType);
			specificHandlerType = handlerType;
		}
		handlerTypes.addAll(Arrays.asList(handlerType.getInterfaces()));
		for(Class<?> currentHandlerType : handlerTypes) {
			final Class<?> targetClass = (specificHandlerType != null ? specificHandlerType : currentHandlerType);
			ReflectionUtils.doWithMethods(currentHandlerType, new ReflectionUtils.MethodCallback() {
				@Override
				public void doWith(Method method) {
					Method specificMethod = ClassUtils.getMostSpecificMethod(method, targetClass);
					Method bridgedMethod = BridgeMethodResolver.findBridgedMethod(specificMethod);
					if(handlerMethodFilter.matches(specificMethod) &&
							(bridgedMethod == specificMethod || !handlerMethodFilter.matches(bridgedMethod))) {
						handlerMethods.add(specificMethod);
					}
				}
			}, ReflectionUtils.USER_DECLARED_METHODS);
		}
		return handlerMethods;
	}

}
