/*
 * Copyright (c) 2011-2013 the original author or authors.
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

package reactor.core.dynamic;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import reactor.Fn;
import reactor.convert.Converter;
import reactor.core.Reactor;
import reactor.core.dynamic.annotation.Dispatcher;
import reactor.core.dynamic.annotation.Notify;
import reactor.core.dynamic.annotation.On;
import reactor.core.dynamic.reflect.MethodNotificationKeyResolver;
import reactor.core.dynamic.reflect.MethodSelectorResolver;
import reactor.core.dynamic.reflect.SimpleMethodNotificationKeyResolver;
import reactor.core.dynamic.reflect.SimpleMethodSelectorResolver;
import reactor.fn.Consumer;
import reactor.fn.ConsumerInvoker;
import reactor.fn.ConverterAwareConsumerInvoker;
import reactor.fn.Event;
import reactor.fn.Function;
import reactor.fn.Registration;
import reactor.fn.Selector;
import reactor.fn.dispatch.BlockingQueueDispatcher;
import reactor.fn.dispatch.RingBufferDispatcher;
import reactor.fn.dispatch.SynchronousDispatcher;
import reactor.fn.dispatch.ThreadPoolExecutorDispatcher;
import reactor.support.Assert;

/**
 * A {@literal DynamicReactorFactory} is responsible for generating a {@link Proxy} based on the given interface, that
 * intercepts calls to the interface and translates them into the appropriate {@link Reactor#on(reactor.fn.Selector,
 * reactor.fn.Consumer)} or {@link Reactor#notify(Object, Event)} calls.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class DynamicReactorFactory<T extends DynamicReactor> {

	private final Class<T> type;
	private final List<MethodSelectorResolver>        selectorResolvers;
	private final List<MethodNotificationKeyResolver> notificationKeyResolvers;
	private final Map<Method, DynamicMethod>          dynamicMethods           = new HashMap<Method, DynamicMethod>();
	private volatile ConsumerInvoker                     consumerInvoker          = new ConverterAwareConsumerInvoker();
	private volatile Converter converter;

	public DynamicReactorFactory(Class<T> type, List<MethodSelectorResolver> selectorResolvers, List<MethodNotificationKeyResolver> notificationKeyResolvers) {
		this.type = type;
		this.selectorResolvers = selectorResolvers;
		this.notificationKeyResolvers = notificationKeyResolvers;
	}

	public DynamicReactorFactory(Class<T> type) {
		this(type, Arrays.<MethodSelectorResolver>asList(new SimpleMethodSelectorResolver()), Arrays.<MethodNotificationKeyResolver>asList(new SimpleMethodNotificationKeyResolver()));
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
	 * Set the {@link ConsumerInvoker} to use when invoking {@link Consumer Consumers}.
	 *
	 * @param consumerInvoker The {@link ConsumerInvoker} to use.
	 * @return {@literal this}
	 */
	public DynamicReactorFactory<T> setConsumerInvoker(ConsumerInvoker consumerInvoker) {
		Assert.notNull(consumerInvoker, "ConsumerInvoker cannot be null.");
		this.consumerInvoker = consumerInvoker;
		return this;
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
	 * Set the {@link Converter} to use when coercing arguments.
	 *
	 * @param converter The {@link Converter} to use.
	 * @return {@literal this}
	 */
	public DynamicReactorFactory<T> setConverter(Converter converter) {
		this.converter = converter;
		return this;
	}

	public T create() {
		return create(new Reactor());
	}

	/**
	 * Generate a {@link Proxy} based on the given interface.
	 *
	 * @return A proxy based on {@link #type}.
	 */
	@SuppressWarnings({"unchecked"})
	public T create(Reactor reactor) {
		return (T) Proxy.newProxyInstance(
				DynamicReactorFactory.class.getClassLoader(),
				new Class[]{type},
				new ReactorInvocationHandler<T>(type)
		);
	}

	private class ReactorInvocationHandler<U> implements InvocationHandler {
		private final Map<Method, Selector> selectors        = new HashMap<Method, Selector>();
		private final Map<Method, Object>   notificationKeys = new HashMap<Method, Object>();

		private final Reactor reactor;

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
							reactor.fn.Selector sel = msr.apply(m);
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
					reg = reactor.on(sel, Fn.<Event<Object>>compose((Runnable) arg));
				} else if (Callable.class.isInstance(arg)) {
					reg = reactor.receive(sel, Fn.<Event<Object>>compose((Callable) arg));
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
					reactor.notify(key, (Event.class.isInstance(args[0]) ? (Event) args[0] : Fn.event(args[0])));
				} else {
					// TODO: handle multiple args
				}

				return (dm.returnsProxy ? proxy : null);
			} else {
				throw new NoSuchMethodError(method.getName());
			}
		}

		private Reactor createReactor(Dispatcher dispatcherType) {
			reactor.fn.dispatch.Dispatcher dispatcher = null;
			if (dispatcherType != null) {
				switch (dispatcherType.value()) {
					case WORKER:
						dispatcher = new BlockingQueueDispatcher();
						break;
					case THREAD_POOL:
						dispatcher = new ThreadPoolExecutorDispatcher();
						break;
					case ROOT:
						dispatcher = new RingBufferDispatcher();
						break;
					case SYNC:
						dispatcher = new SynchronousDispatcher();
						break;
				}
				dispatcher.start();
			}
			return new Reactor(dispatcher);
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
