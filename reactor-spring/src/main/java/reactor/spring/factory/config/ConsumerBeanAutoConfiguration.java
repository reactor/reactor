package reactor.spring.factory.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.BridgeMethodResolver;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.convert.ConversionService;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.common.TemplateAwareExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
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

import static reactor.event.selector.JsonPathSelector.jsonPathSelector;
import static reactor.event.selector.Selectors.*;

/**
 * {@link org.springframework.context.ApplicationListener} implementation that finds beans registered in the current
 * {@link org.springframework.context.ApplicationContext} that look like a {@link reactor.spring.annotation.Consumer}
 * bean and interrogates it for event handling methods.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ConsumerBeanAutoConfiguration implements ApplicationListener<ContextRefreshedEvent>,
                                                      ApplicationContextAware {

	public static final String REACTOR_CONVERSION_SERVICE_BEAN_NAME = "reactorConversionService";

	public static final ReflectionUtils.MethodFilter CONSUMER_METHOD_FILTER = new ReflectionUtils.MethodFilter() {
		@Override
		public boolean matches(Method method) {
			return null != AnnotationUtils.findAnnotation(method, Selector.class);
		}
	};

	private static final Logger LOG = LoggerFactory.getLogger(ConsumerBeanAutoConfiguration.class);

	private ApplicationContext            appCtx;
	private BeanResolver                  beanResolver;
	private ConversionService             conversionService;
	private TemplateAwareExpressionParser expressionParser;

	private boolean started = false;

	public ConsumerBeanAutoConfiguration() {
		this.expressionParser = new SpelExpressionParser();
	}

	@Override
	public void setApplicationContext(ApplicationContext appCtx) throws BeansException {
		this.appCtx = appCtx;
	}

	@Override
	public void onApplicationEvent(ContextRefreshedEvent ev) {
		ApplicationContext ctx = ev.getApplicationContext();
		if(ctx != appCtx) {
			return;
		}

		synchronized(this) {
			if(started) {
				return;
			}

			if(null == beanResolver) {
				beanResolver = new BeanFactoryResolver(ctx);
			}

			if(null == conversionService) {
				try {
					conversionService = ctx.getBean(REACTOR_CONVERSION_SERVICE_BEAN_NAME, ConversionService.class);
				} catch(BeansException be) {
					if(LOG.isDebugEnabled()) {
						LOG.debug(REACTOR_CONVERSION_SERVICE_BEAN_NAME + " has not been found in the context. Skipping.");
					}
				}
			}

			Set<Method> methods;
			Class<?> type;
			for(String beanName : ctx.getBeanDefinitionNames()) {
				type = ctx.getType(beanName);
				methods = findHandlerMethods(type, CONSUMER_METHOD_FILTER);
				if(methods != null && methods.size() > 0) {
					wireBean(ctx.getBean(beanName), methods);
				}
			}

			started = true;
		}

	}

	@SuppressWarnings("unchecked")
	public void wireBean(final Object bean, final Set<Method> methods) {
		if(methods == null || methods.isEmpty()) {
			return;
		}

		Consumer consumer;
		Observable reactor;
		Selector selectorAnno;
		ReplyTo replyToAnno;
		reactor.event.selector.Selector selector;

		for(final Method method : methods) {
			//scanAnnotation method
			selectorAnno = AnnotationUtils.findAnnotation(method, Selector.class);
			replyToAnno = AnnotationUtils.findAnnotation(method, ReplyTo.class);
			reactor = fetchObservable(selectorAnno, bean);
			selector = fetchSelector(selectorAnno, bean, method);

			//register [replyTo]consumer
			Object replyTo = replyToAnno != null ? parseReplyTo(replyToAnno, bean) : null;
			Invoker handler = new Invoker(method, bean, conversionService);
			consumer = null != replyToAnno ?
			           new ReplyToServiceConsumer(reactor, replyTo, handler) :
			           new ServiceConsumer(handler);

			if(LOG.isDebugEnabled()) {
				LOG.debug("Attaching Consumer to Reactor[" + reactor + "] using Selector[" + selector + "]");
			}

			if(null == selector) {
				reactor.on(consumer);
			} else {
				reactor.on(selector, consumer);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private <T> T expression(String selector, Object bean) {
		if(selector == null) {
			return null;
		}

		StandardEvaluationContext evalCtx = new StandardEvaluationContext();
		evalCtx.setRootObject(bean);
		evalCtx.setBeanResolver(beanResolver);

		return (T)expressionParser.parseExpression(selector).getValue(evalCtx);
	}

	protected Observable fetchObservable(Selector selectorAnno, Object bean) {
		return expression(selectorAnno.reactor(), bean);
	}

	protected Object parseSelector(Selector selector, Object bean, Method method) {
		if(!StringUtils.hasText(selector.value())) {
			return method.getName();
		}

		try {
			return expression(selector.value(), bean);
		} catch(Exception e) {
			return selector.value();
		}
	}

	protected Object parseReplyTo(ReplyTo selector, Object bean) {
		if(StringUtils.isEmpty(selector.value())) {
			return null;
		}
		try {
			return expression(selector.value(), bean);
		} catch(EvaluationException ee) {
			return selector.value();
		}
	}

	protected reactor.event.selector.Selector fetchSelector(Selector selectorAnno, Object bean, Method method) {
		Object sel = parseSelector(selectorAnno, bean, method);
		try {
			switch(selectorAnno.type()) {
				case OBJECT:
					return object(sel);
				case REGEX:
					return regex(sel.toString());
				case URI:
					return uri(sel.toString());
				case TYPE:
					try {
						return type(Class.forName(sel.toString()));
					} catch(ClassNotFoundException e) {
						throw new IllegalArgumentException(e.getMessage(), e);
					}
				case JSON_PATH:
					return jsonPathSelector(sel.toString());
			}
		} catch(EvaluationException e) {
			if(LOG.isTraceEnabled()) {
				LOG.trace("Creating ObjectSelector for '" + sel + "' due to " + e.getMessage(), e);
			}
		}
		return object(sel);
	}

	protected final static class ReplyToServiceConsumer implements Consumer<Event> {

		final private Observable reactor;
		final private Object     replyToKey;
		final private Invoker    handler;

		ReplyToServiceConsumer(Observable reactor, Object replyToKey, Invoker handler) {
			this.reactor = reactor;
			this.replyToKey = replyToKey;
			this.handler = handler;
		}

		public Observable getReactor() {
			return reactor;
		}

		public Object getReplyToKey() {
			return replyToKey;
		}

		public Invoker getHandler() {
			return handler;
		}

		@Override
		public void accept(Event ev) {
			Object result = handler.apply(ev);
			Object _replyToKey = replyToKey != null ? replyToKey : ev.getReplyTo();
			if(_replyToKey != null) {
				reactor.notify(_replyToKey, Event.wrap(result));
			}
		}
	}

	protected final static class ServiceConsumer implements Consumer<Event> {
		final private Invoker handler;

		ServiceConsumer(Invoker handler) {
			this.handler = handler;
		}

		public Invoker getHandler() {
			return handler;
		}

		@Override
		public void accept(Event ev) {
			handler.apply(ev);
		}
	}

	protected final static class Invoker implements Function<Event, Object> {

		final private Method            method;
		final private Object            bean;
		final private Class<?>[]        argTypes;
		final private ConversionService conversionService;

		Invoker(Method method, Object bean, ConversionService conversionService) {
			this.method = method;
			this.bean = bean;
			this.conversionService = conversionService;
			this.argTypes = method.getParameterTypes();
		}

		public Method getMethod() {
			return method;
		}

		public Object getBean() {
			return bean;
		}

		public Class<?>[] getArgTypes() {
			return argTypes;
		}

		@Override
		public Object apply(Event ev) {
			if(argTypes.length == 0) {
				if(LOG.isDebugEnabled()) {
					LOG.debug("Invoking method[" + method + "] on " + bean.getClass() + " using " + ev);
				}
				return ReflectionUtils.invokeMethod(method, bean);
			}

			if(argTypes.length > 1) {
				throw new IllegalStateException("Multiple parameters not yet supported.");
			}

			if(Event.class.isAssignableFrom(argTypes[0])) {
				if(LOG.isDebugEnabled()) {
					LOG.debug("Invoking method[" + method + "] on " + bean.getClass() + " using " + ev);
				}
				return ReflectionUtils.invokeMethod(method, bean, ev);
			}

			if(null == ev.getData() || argTypes[0].isAssignableFrom(ev.getData().getClass())) {
				if(LOG.isDebugEnabled()) {
					LOG.debug("Invoking method[" + method + "] on " + bean.getClass() + " using " + ev.getData());
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
					LOG.debug("Invoking method[" + method + "] on " + bean.getClass() + " using " + convertedObj);
				}
				return ReflectionUtils.invokeMethod(method, bean, convertedObj);
			}

			throw new IllegalArgumentException("Cannot invoke method " + method + " passing parameter " + ev.getData());
		}
	}

	public static Set<Method> findHandlerMethods(Class<?> handlerType,
	                                             final ReflectionUtils.MethodFilter handlerMethodFilter) {
		final Set<Method> handlerMethods = new LinkedHashSet<Method>();

		if(handlerType == null) {
			return handlerMethods;
		}

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
