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
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.expression.BeanFactoryResolver;
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
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import reactor.core.Reactor;
import reactor.function.Consumer;
import reactor.event.Event;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.spring.context.annotation.On;

/**
 * @author Jon Brisbin
 */
public class ConsumerBeanPostProcessor implements BeanPostProcessor,
																									BeanFactoryAware {

	private static final List<MethodResolver> METHOD_RESOLVERS = Arrays.<MethodResolver>asList(new ReactorsMethodResolver());
	private BeanResolver beanResolver;
	private TemplateAwareExpressionParser expressionParser  = new SpelExpressionParser();
	@Autowired(required = false)
	private ConversionService             conversionService = new DefaultFormattingConversionService();

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		beanResolver = new BeanFactoryResolver(beanFactory);
	}

	@Override
	public Object postProcessBeforeInitialization(final Object bean,
																								String beanName)
			throws BeansException {
		ReflectionUtils.doWithMethods(bean.getClass(), new ReflectionUtils.MethodCallback() {
			@Override
			public void doWith(final Method method) throws IllegalArgumentException,
																										 IllegalAccessException {
				Annotation anno = AnnotationUtils.findAnnotation(method, On.class);
				if (null == anno) {
					return;
				}

				StandardEvaluationContext evalCtx = new StandardEvaluationContext();
				evalCtx.setRootObject(bean);
				evalCtx.setBeanResolver(beanResolver);
				evalCtx.setMethodResolvers(METHOD_RESOLVERS);

				On onAnno = (On) anno;

				Object reactorObj = null;
				if (StringUtils.hasText(onAnno.reactor())) {
					Expression reactorExpr = expressionParser.parseExpression(onAnno.reactor());
					reactorObj = reactorExpr.getValue(evalCtx);
				}

				Object selObj;
				if (StringUtils.hasText(onAnno.selector())) {
					try {
						Expression selectorExpr = expressionParser.parseExpression(onAnno.selector());
						selObj = selectorExpr.getValue(evalCtx);
					} catch (EvaluationException e) {
						selObj = Selectors.$(onAnno.selector());
					}
				} else {
					selObj = Selectors.$(method.getName());
				}

				Consumer<Event<Object>> handler = new Consumer<Event<Object>>() {
					Class<?>[] argTypes = method.getParameterTypes();

					@Override
					public void accept(Event<Object> ev) {
						if (argTypes.length == 0) {
							ReflectionUtils.invokeMethod(method, bean);
							return;
						}

						if (!argTypes[0].isAssignableFrom(ev.getClass())
								&& conversionService.canConvert(ev.getClass(), argTypes[0])) {
							ReflectionUtils.invokeMethod(method, bean, conversionService.convert(ev, argTypes[0]));
						} else {
							ReflectionUtils.invokeMethod(method, bean, ev);
							return;
						}

						if (null == ev.getData() || argTypes[0].isAssignableFrom(ev.getData().getClass())) {
							ReflectionUtils.invokeMethod(method, bean, ev.getData());
							return;
						}

						if (conversionService.canConvert(ev.getData().getClass(), argTypes[0])) {
							ReflectionUtils.invokeMethod(method, bean, conversionService.convert(ev.getData(), argTypes[0]));
							return;
						}

						throw new IllegalArgumentException("Cannot invoke method " + method + " passing parameter " + ev.getData());
					}
				};

				if (!(selObj instanceof Selector)) {
					throw new IllegalArgumentException(selObj + ", referred to by the expression '"
																								 + onAnno.selector() + "', is not a Selector");
				}
				if (null == reactorObj) {
					throw new IllegalStateException("Cannot register handler with null Reactor");
				} else {
					((Reactor) reactorObj).on((Selector) selObj, handler);
				}
			}
		});
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
			if ("$".equals(name)) {
				return new MethodExecutor() {
					@Override
					public TypedValue execute(EvaluationContext context,
																		Object target,
																		Object... arguments) throws AccessException {
						if (arguments.length != 1
								|| null == arguments[0]
								|| !Serializable.class.isAssignableFrom(arguments[0].getClass())) {
							return null;
						}
						Selector sel = Selectors.$(arguments[0]);
						return new TypedValue(sel, TypeDescriptor.valueOf(sel.getClass()));
					}
				};
			} else if ("R".equals(name)) {
				return new MethodExecutor() {
					@Override
					public TypedValue execute(EvaluationContext context,
																		Object target,
																		Object... arguments) throws AccessException {
						if (arguments.length != 1 || null == arguments[0]) {
							return null;
						}
						Selector sel = Selectors.r(arguments[0].toString());
						return new TypedValue(sel, TypeDescriptor.valueOf(sel.getClass()));
					}
				};
			} else if ("T".equals(name)) {
				return new MethodExecutor() {
					@Override
					public TypedValue execute(EvaluationContext context,
																		Object target,
																		Object... arguments) throws AccessException {
						if (arguments.length != 1
								|| null == arguments[0]
								|| !Class.class.isAssignableFrom(arguments[0].getClass())) {
							return null;
						}
						Selector sel = Selectors.t((Class<?>) arguments[0]);
						return new TypedValue(sel, TypeDescriptor.valueOf(sel.getClass()));
					}
				};
			}

			return null;
		}
	}

}
