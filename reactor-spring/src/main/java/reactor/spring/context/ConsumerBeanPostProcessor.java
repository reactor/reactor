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

package reactor.spring.context;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.expression.AccessException;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.MethodExecutor;
import org.springframework.expression.MethodResolver;
import org.springframework.expression.TypedValue;
import org.springframework.expression.common.TemplateAwareExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import reactor.event.Event;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Observable;
import reactor.spring.context.annotation.On;
import reactor.util.StringUtils;

/**
 * @author Jon Brisbin
 */
public class ConsumerBeanPostProcessor implements BeanPostProcessor,
                                                  BeanFactoryAware,
                                                  Ordered {

	private static final Logger               LOG              = LoggerFactory.getLogger(ConsumerBeanPostProcessor.class);
	private static final List<MethodResolver> METHOD_RESOLVERS = Arrays.<MethodResolver>asList(new ReactorsMethodResolver());
	private BeanResolver beanResolver;
	private TemplateAwareExpressionParser expressionParser = new SpelExpressionParser();
	private final ConversionService conversionService;

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
	public Object postProcessBeforeInitialization(final Object bean,
	                                              String beanName) throws BeansException {
		ReflectionUtils.doWithMethods(
				bean.getClass(),
				new ReflectionUtils.MethodCallback() {
					@Override
					public void doWith(final Method method) throws IllegalArgumentException,
					                                               IllegalAccessException {
						StandardEvaluationContext evalCtx = new StandardEvaluationContext();
						evalCtx.setRootObject(bean);
						evalCtx.setBeanResolver(beanResolver);
						evalCtx.setMethodResolvers(METHOD_RESOLVERS);

						On onAnno = AnnotationUtils.findAnnotation(method, On.class);

						Observable reactor = (Observable)expressionParser.parseExpression(onAnno.reactor()).getValue(evalCtx);
						Assert.notNull(reactor, "Reactor cannot be null");

						Selector selector = null;
						if(StringUtils.hasText(onAnno.value())) {
							try {
								Expression selectorExpr = expressionParser.parseExpression(onAnno.value());
								Object selObj = selectorExpr.getValue(evalCtx);

								switch(onAnno.type()) {
									case OBJECT:
										selector = Selectors.object(selObj);
										break;
									case REGEX:
										selector = Selectors.regex(selObj.toString());
										break;
									case URI:
										selector = Selectors.uri(selObj.toString());
										break;
									case TYPE:
										if(selObj instanceof Class) {
											selector = Selectors.type((Class<?>)selObj);
										} else {
											try {
												selector = Selectors.type(Class.forName(selObj.toString()));
											} catch(ClassNotFoundException e) {
												throw new IllegalArgumentException(e.getMessage(), e);
											}
										}
										break;
								}
							} catch(EvaluationException e) {
								if(LOG.isTraceEnabled()) {
									LOG.trace("Creating ObjectSelector for '" + onAnno.value() + "' due to " + e.getMessage(), e);
								}
								selector = Selectors.object(onAnno.value());
							}
						}

						Consumer<Event<Object>> handler = new Consumer<Event<Object>>() {
							Class<?>[] argTypes = method.getParameterTypes();

							@Override
							public void accept(Event<Object> ev) {
								if(argTypes.length == 0) {
									if(LOG.isDebugEnabled()) {
										LOG.debug("Invoking method[" + method + "] on " + bean + " using " + ev);
									}
									ReflectionUtils.invokeMethod(method, bean);
									return;
								}

								if(argTypes.length > 1) {
									// TODO: handle more than one parameter
									throw new IllegalStateException("Multiple parameters not yet supported.");
								}

								if(null == ev.getData() || argTypes[0].isAssignableFrom(ev.getData().getClass())) {
									if(LOG.isDebugEnabled()) {
										LOG.debug("Invoking method[" + method + "] on " + bean + " using " + ev.getData());
									}
									ReflectionUtils.invokeMethod(method, bean, ev.getData());
									return;
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
									ReflectionUtils.invokeMethod(method, bean, convertedObj);
									return;
								}

								throw new IllegalArgumentException("Cannot invoke method " + method + " passing parameter " + ev.getData());
							}
						};

						if(LOG.isDebugEnabled()) {
							LOG.debug("Attaching Selector[" + selector + "] to Reactor[" + reactor + "] using Consumer[" + handler + "]");
						}
						if(null == selector) {
							reactor.on(handler);
						} else {
							reactor.on(selector, handler);
						}
					}
				},
				new ReflectionUtils.MethodFilter() {
					@Override public boolean matches(Method method) {
						return null != AnnotationUtils.findAnnotation(method, On.class);
					}
				}
		);
		return bean;
	}

	@Override
	public Object postProcessAfterInitialization(Object bean,
	                                             String beanName)
			throws BeansException {
		return bean;
	}

	private static class ReactorsMethodResolver implements MethodResolver {
		@Override
		public MethodExecutor resolve(EvaluationContext context,
		                              Object targetObject,
		                              String name,
		                              List<TypeDescriptor> argumentTypes) throws AccessException {
			if("$".equals(name)) {
				return new MethodExecutor() {
					@Override
					public TypedValue execute(EvaluationContext context,
					                          Object target,
					                          Object... arguments) throws AccessException {
						if(arguments.length != 1
								|| null == arguments[0]
								|| !Serializable.class.isAssignableFrom(arguments[0].getClass())) {
							return null;
						}
						Selector sel = Selectors.$(arguments[0]);
						return new TypedValue(sel, TypeDescriptor.valueOf(sel.getClass()));
					}
				};
			} else if("R".equals(name)) {
				return new MethodExecutor() {
					@Override
					public TypedValue execute(EvaluationContext context,
					                          Object target,
					                          Object... arguments) throws AccessException {
						if(arguments.length != 1 || null == arguments[0]) {
							return null;
						}
						Selector sel = Selectors.R(arguments[0].toString());
						return new TypedValue(sel, TypeDescriptor.valueOf(sel.getClass()));
					}
				};
			} else if("T".equals(name)) {
				return new MethodExecutor() {
					@Override
					public TypedValue execute(EvaluationContext context,
					                          Object target,
					                          Object... arguments) throws AccessException {
						if(arguments.length != 1
								|| null == arguments[0]
								|| !Class.class.isAssignableFrom(arguments[0].getClass())) {
							return null;
						}
						Selector sel = Selectors.T((Class<?>)arguments[0]);
						return new TypedValue(sel, TypeDescriptor.valueOf(sel.getClass()));
					}
				};
			}

			return null;
		}
	}

}
