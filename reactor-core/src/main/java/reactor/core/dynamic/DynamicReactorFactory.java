/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.core.dynamic;

import reactor.convert.Converter;
import reactor.core.Environment;
import reactor.core.dynamic.annotation.Dispatcher;
import reactor.core.dynamic.annotation.Notify;
import reactor.core.dynamic.annotation.On;
import reactor.core.dynamic.reflect.MethodNotificationKeyResolver;
import reactor.core.dynamic.reflect.MethodSelectorResolver;
import reactor.core.dynamic.reflect.SimpleMethodNotificationKeyResolver;
import reactor.core.dynamic.reflect.SimpleMethodSelectorResolver;
import reactor.core.spec.ReactorSpec;
import reactor.core.spec.Reactors;
import reactor.event.Event;
import reactor.event.EventBus;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.registry.Registration;
import reactor.event.routing.ArgumentConvertingConsumerInvoker;
import reactor.event.routing.ConsumerInvoker;
import reactor.event.selector.Selector;
import reactor.function.Consumer;
import reactor.function.Function;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * A {@literal DynamicReactorFactory} is responsible for generating a {@link Proxy} based on the given interface, that
 * intercepts calls to the interface and translates them into the appropriate {@link
 * reactor.event.EventBus#on(reactor.event.selector.Selector, reactor.function.Consumer)} or {@link reactor.event.EventBus#notify(Object, Event)}
 * calls. Methods that are translated to {@code on} calls should take a single argument that is a {@link Consumer},
 * {@link Function}, {@link Runnable}, or {@link Callable}. Methods that are translated to {@code notify} calls should
 * take zero or one arguments. If the method takes zero arguments it will be translated to {@link
 * reactor.event.EventBus#notify(Object)}. IF the method takes one argument it will be translated to {@link reactor.event.EventBus#notify(Object,
 * Event)}, {@link Event#wrap(Object) wrapping} the argument in an {@link Event} if necessary.
 * <p/>
 * The translation of calls on the interface to calls to {@code on} and the selector that is used is determined by the
 * {@link MethodSelectorResolver}s. The translation of calls on the interface to calls to {@link reactor.event.EventBus#notify(Object,
 * Event) notify} is determined by the {@link MethodNotificationKeyResolver}s.
 * <p/>
 * By default, the creation of the selector for {@code on} calls will look for the {@link On} annotation. In its
 * absence, if the method name begins with {@code on}, the method name minus the on prefix and with the first character
 * lower-cased, will be used to create the selector. For example, the selector for a method named {@code onAlpha} will
 * be {@code "alpha"}.
 * <p/>
 * By default, the translation for {@code notify} calls will look for the {@link Notify} annotation. In its absence, if
 * the method name begins with {@code notify}, the method name minus the notify prefix and with the first character
 * lower-cased, will be used to create the notification key. For example, the notification key for a method named
 * {@code notifyAlpha} will be {@code "alpha"}.
 *
 * @param <T>
 * 		The type to proxy
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class DynamicReactorFactory<T extends DynamicReactor> {

	private final Environment                         env;
	private final Class<T>                            type;
	private final List<MethodSelectorResolver>        selectorResolvers;
	private final List<MethodNotificationKeyResolver> notificationKeyResolvers;
	private final ConsumerInvoker                     consumerInvoker;
	private final Converter                           converter;
	private final Map<Method, DynamicMethod> dynamicMethods = new HashMap<Method, DynamicMethod>();


	/**
	 * Creates a new DynamicReactorFactory that will use the given {@code env} when creating its {@link reactor.event.EventBus}. The
	 * proxy
	 * that is generated will be based upon the given {@code type}. A {@link SimpleMethodSelectorResolver} will be used to
	 * create selectors for proxied methods. A {@link SimpleMethodNotificationKeyResolver} will be used to create
	 * notification keys for proxied methods.
	 *
	 * @param env
	 * 		The Environment to use when creating the underlying Reactor.
	 * @param type
	 * 		The type of the proxy
	 */
	public DynamicReactorFactory(Environment env,
	                             Class<T> type) {
		this(env,
		     type,
		     Arrays.<MethodSelectorResolver>asList(new SimpleMethodSelectorResolver()),
		     Arrays.<MethodNotificationKeyResolver>asList(new SimpleMethodNotificationKeyResolver()),
		     new ArgumentConvertingConsumerInvoker(null),
		     null);
	}

	/**
	 * Creates a new DynamicReactorFactory that will use the given {@code env} when creating its {@link reactor.event.EventBus}. The
	 * proxy
	 * that is generated will be based upon the given {@code type}. The {@code selectorResolvers} will be used to create
	 * selectors for proxied methods and the {@code notificationKeyResolvers} will be used to create notification keys for
	 * proxied methods.
	 *
	 * @param env
	 * 		The Environment to use when creating the underlying Reactor.
	 * @param type
	 * 		The type of the proxy
	 * @param selectorResolvers
	 * 		used to resolve the selectors for proxied methods
	 * @param notificationKeyResolvers
	 * 		used to resolve the notification keys for proxied methods
	 */
	public DynamicReactorFactory(Environment env,
	                             Class<T> type,
	                             List<MethodSelectorResolver> selectorResolvers,
	                             List<MethodNotificationKeyResolver> notificationKeyResolvers,
	                             ConsumerInvoker consumerInvoker,
	                             Converter converter) {
		this.env = env;
		this.type = type;
		this.selectorResolvers = selectorResolvers;
		this.notificationKeyResolvers = notificationKeyResolvers;
		this.consumerInvoker = consumerInvoker;
		this.converter = converter;
	}

	/**
	 * Get the list of {@link MethodSelectorResolver}s in use.
	 *
	 * @return The {@link MethodSelectorResolver}s in use.
	 */
	public List<MethodSelectorResolver> getSelectorResolvers() {
		return Collections.unmodifiableList(selectorResolvers);
	}

	/**
	 * Get the {@link ConsumerInvoker} to use when invoking {@link Consumer Consumers}.
	 *
	 * @return The {@link ConsumerInvoker} in use.
	 */
	public ConsumerInvoker getConsumerInvoker() {
		return consumerInvoker;
	}

	/**
	 * Get the {@link Converter} to use when coercing arguments.
	 *
	 * @return The {@link Converter} to use.
	 */
	public Converter getConverter() {
		return converter;
	}

	/**
	 * Generate a {@link Proxy} based on the type used to create this factory.
	 *
	 * @return A proxy based on the type used to create the factory
	 */
	@SuppressWarnings("unchecked")
	public T create() {
		return (T)Proxy.newProxyInstance(
				DynamicReactorFactory.class.getClassLoader(),
				new Class[]{type},
				new ReactorInvocationHandler<T>(type)
		);
	}

	private class ReactorInvocationHandler<U> implements InvocationHandler {
		private final Map<Method, Selector> selectors        = new HashMap<Method, Selector>();
		private final Map<Method, Object>   notificationKeys = new HashMap<Method, Object>();

		private final EventBus reactor;

		private ReactorInvocationHandler(Class<U> type) {
			Dispatcher d = find(type, Dispatcher.class);
			this.reactor = createReactor(d);

			for (Method m : type.getDeclaredMethods()) {
				if (m.getDeclaringClass() == Object.class || m.getName().contains("$")) {
					continue;
				}

				DynamicMethod dm = new DynamicMethod();

				dm.returnsRegistration = Registration.class.isAssignableFrom(m.getReturnType());
				dm.returnsProxy = type.isAssignableFrom(m.getReturnType());

				if (isOn(m)) {
					for (MethodSelectorResolver msr : selectorResolvers) {
						if (msr.supports(m)) {
							Selector sel = msr.apply(m);
							if (null != sel) {
								selectors.put(m, sel);
								dynamicMethods.put(m, dm);
								break;
							}
						}
					}
				} else if (isNotify(m)) {
					for (MethodNotificationKeyResolver notificationKeyResolver : notificationKeyResolvers) {
						if (notificationKeyResolver.supports(m)) {
							String notificationKey = notificationKeyResolver.apply(m);
							if (null != notificationKey) {
								notificationKeys.put(m, notificationKey);
								dynamicMethods.put(m, dm);
								break;
							}
						}
					}
				}
			}
		}

		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public Object invoke(Object proxy, Method method, final Object[] args) throws Throwable {
			final DynamicMethod dm = dynamicMethods.get(method);

			if (isOn(method)) {
				Selector sel = selectors.get(method);
				if (null == sel) {
					return proxy;
				}

				if (args.length > 1) {
					throw new IllegalArgumentException("Only pass a single Consumer, Function, Runnable, or Callable");
				}

				final Object arg = args[0];

				Registration reg = null;
				if (Consumer.class.isInstance(arg)) {
					reg = reactor.on(sel, (Consumer) arg);
				} else if (Function.class.isInstance(arg)) {
					reg = reactor.receive(sel, (Function) arg);
				} else if (Runnable.class.isInstance(arg)) {
					reg = reactor.on(sel, new Consumer<Event<?>>() {
						@Override
						public void accept(Event<?> event) {
							((Runnable) arg).run();
						}
					});
				} else if (Callable.class.isInstance(arg)) {
					reg = reactor.receive(sel, new Function<Event<?>, Object>() {
						@Override
						public Object apply(Event<?> event) {
							try {
								return ((Callable) arg).call();
							} catch (Exception e) {
								reactor.notify(e.getClass(), Event.wrap(e));
							}
							return null;
						}
					});
				} else if (null == converter || !converter.canConvert(arg.getClass(), Consumer.class)) {
					throw new IllegalArgumentException(
							String.format("No Converter available to convert '%s' to Consumer", arg.getClass().getName())
					);
				}

				return (dm.returnsRegistration ? reg : dm.returnsProxy ? proxy : null);
			} else if (isNotify(method)) {
				Object key = notificationKeys.get(method);
				if (null == key) {
					return proxy;
				}

				if (args.length == 0) {
					reactor.notify(key);
				} else if (args.length == 1) {
					reactor.notify(key, (Event.class.isInstance(args[0]) ? (Event) args[0] : Event.wrap(args[0])));
				} else {
					// TODO: handle multiple args
				}

				return (dm.returnsProxy ? proxy : null);
			} else {
				throw new NoSuchMethodError(method.getName());
			}
		}

		private EventBus createReactor(Dispatcher dispatcherAnnotation) {
			ReactorSpec reactorSpec = Reactors.reactor().env(env);
			if (dispatcherAnnotation != null) {
				if ("sync".equals(dispatcherAnnotation.value())) {
					reactorSpec.dispatcher(SynchronousDispatcher.INSTANCE);
				} else {
					reactorSpec.dispatcher(dispatcherAnnotation.value());
				}
			}
			return reactorSpec.get();
		}
	}

	private static boolean isOn(Method m) {
		return m.getName().startsWith("on") || null != m.getAnnotation(On.class);
	}

	private static boolean isNotify(Method m) {
		return m.getName().startsWith("notify") || null != m.getAnnotation(Notify.class);
	}

	@SuppressWarnings("unchecked")
	private static <T extends Annotation> T find(Class<?> type, Class<T> annoType) {
		if (type.getDeclaredAnnotations().length > 0) {
			for (Annotation anno : type.getDeclaredAnnotations()) {
				if (annoType.isAssignableFrom(anno.getClass())) {
					return ((T) anno);
				}
			}
		}
		return null;
	}

	private static final class DynamicMethod {
		boolean returnsRegistration = false;
		boolean returnsProxy        = true;
	}

}
