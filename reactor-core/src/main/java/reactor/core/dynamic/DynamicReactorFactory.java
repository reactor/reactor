package reactor.core.dynamic;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import reactor.Fn;
import reactor.core.Context;
import reactor.core.R;
import reactor.core.Reactor;
import reactor.core.dynamic.annotation.Dispatcher;
import reactor.core.dynamic.annotation.Notify;
import reactor.core.dynamic.annotation.On;
import reactor.core.dynamic.reflect.MethodArgumentResolver;
import reactor.core.dynamic.reflect.MethodNotificationKeyResolver;
import reactor.core.dynamic.reflect.MethodSelectorResolver;
import reactor.core.dynamic.reflect.SimpleMethodNotificationKeyResolver;
import reactor.core.dynamic.reflect.SimpleMethodSelectorResolver;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.Selector;
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
	private volatile List<MethodSelectorResolver> selectorResolvers = Arrays.<MethodSelectorResolver>asList(
			new SimpleMethodSelectorResolver()
	);

	private volatile List<MethodNotificationKeyResolver> notificationKeyResolvers = Arrays.<MethodNotificationKeyResolver>asList(
			new SimpleMethodNotificationKeyResolver()
	);

	private List<MethodArgumentResolver<?>> methodArgumentResolvers;

	public DynamicReactorFactory(Class<T> type) {
		this.type = type;
	}

	/**
	 * Get the list of {@link MethodSelectorResolver}s in use.
	 *
	 * @return The {@link MethodSelectorResolver}s in use.
	 */
	public List<MethodSelectorResolver> getSelectorResolvers() {
		return selectorResolvers;
	}

	/**
	 * Set the {@link MethodSelectorResolver}s to use to determine what {@link reactor.fn.Selector} maps to what {@link
	 * Method}.
	 *
	 * @param selectorResolvers The {@link MethodSelectorResolver}s to use.
	 * @return {@literal this}
	 */
	public DynamicReactorFactory<T> setSelectorResolvers(List<MethodSelectorResolver> selectorResolvers) {
		Assert.notNull(selectorResolvers, "MethodSelectorResolvers cannot be null.");
		if (selectorResolvers.isEmpty()) {
			return this;
		}
		this.selectorResolvers = selectorResolvers;
		return this;
	}

	/**
	 * Set the {@link MethodSelectorResolver}s to use to determine what {@link reactor.fn.Selector} maps to what {@link
	 * Method}.
	 *
	 * @param selectorResolvers The {@link MethodSelectorResolver}s to use.
	 * @return {@literal this}
	 */
	public DynamicReactorFactory<T> setSelectorResolvers(MethodSelectorResolver... selectorResolvers) {
		if (selectorResolvers.length == 0) {
			return this;
		}
		this.selectorResolvers = Arrays.asList(selectorResolvers);
		return this;
	}

	/**
	 * Set the {@link MethodNotificationKeyResolver}s to use to resolves a {@link Method}'s notification key.
	 *
	 * @param notificationKeyResolvers The {@link MethodNotificationKeyResolver}s to use.
	 * @return {@literal this}
	 */
	public DynamicReactorFactory<T> setNotificationKeyResolvers(List<MethodNotificationKeyResolver> notificationKeyResolvers) {
		Assert.notNull(notificationKeyResolvers, "notificationKeyResolvers cannot be null.");
		if (notificationKeyResolvers.isEmpty()) {
			return this;
		}
		this.notificationKeyResolvers = notificationKeyResolvers;
		return this;
	}

	/**
	 * Set the {@link MethodNotificationKeyResolver}s to use to resolves a {@link Method}'s notification key.
	 *
	 * @param notificationKeyResolvers The {@link MethodNotificationKeyResolver}s to use.
	 * @return {@literal this}
	 */
	public DynamicReactorFactory<T> setNotificationKeyResolvers(MethodNotificationKeyResolver... notificationKeyResolvers) {
		if (notificationKeyResolvers.length == 0) {
			return this;
		}
		this.notificationKeyResolvers = Arrays.asList(notificationKeyResolvers);
		return this;
	}

	public List<MethodArgumentResolver<?>> getMethodArgumentResolvers() {
		return methodArgumentResolvers;
	}

	public DynamicReactorFactory<T> setMethodArgumentResolvers(List<MethodArgumentResolver<?>> methodArgumentResolvers) {
		this.methodArgumentResolvers = methodArgumentResolvers;
		return this;
	}

	public DynamicReactorFactory<T> setMethodArgumentResolvers(MethodArgumentResolver<?>... methodArgumentResolvers) {
		this.methodArgumentResolvers = Arrays.asList(methodArgumentResolvers);
		return this;
	}

	/**
	 * Generate a {@link Proxy} based on the given interface using the default behavior.
	 *
	 * @return A proxy based on {@link #type}.
	 */
	public T create() {
		return create(R.create());
	}

	/**
	 * Generate a {@link Proxy} based on the given interface using the given {@link Reactor} for dispatching events.
	 *
	 * @param reactor The {@link Reactor} to use.
	 * @return A proxy based on {@link #type}.
	 */
	@SuppressWarnings({"unchecked"})
	public T create(Reactor reactor) {
		return (T) Proxy.newProxyInstance(
				DynamicReactorFactory.class.getClassLoader(),
				new Class[]{type},
				new ReactorInvocationHandler<T>(reactor, type)
		);
	}

	private class ReactorInvocationHandler<U> implements InvocationHandler {
		private final Map<Method, Selector> selectors = new HashMap<Method, Selector>();
		private final Map<Method, Object> notificationKeys = new HashMap<Method, Object>();

		private final Reactor reactor;

		private ReactorInvocationHandler(Reactor reactor, Class<U> type) {
			this.reactor = reactor;
			Dispatcher d = find(type, Dispatcher.class);
			if (null != d) {
				switch (d.value()) {
					case WORKER:
						reactor.setDispatcher(Context.nextWorkerDispatcher());
						break;
					case THREAD_POOL:
						reactor.setDispatcher(Context.threadPoolDispatcher());
						break;
					case ROOT:
						reactor.setDispatcher(Context.rootDispatcher());
						break;
					case SYNC:
						reactor.setDispatcher(Context.synchronousDispatcher());
						break;
				}
			}
			for (Method m : type.getDeclaredMethods()) {
				if (m.getDeclaringClass() == Object.class || m.getName().contains("$")) {
					continue;
				}

				if (isOn(m)) {
					for (MethodSelectorResolver msr : selectorResolvers) {
						if (msr.supports(m)) {
							reactor.fn.Selector sel = msr.apply(m);
							if (null != sel) {
								selectors.put(m, sel);
							}
							break;
						}
					}
				} else if (isNotify(m)) {
					for (MethodNotificationKeyResolver notificationKeyResolver : notificationKeyResolvers) {
						if (notificationKeyResolver.supports(m)) {
							String notificationKey = notificationKeyResolver.apply(m);
							if (null != notificationKey) {
								notificationKeys.put(m, notificationKey);
							}
						}
					}
				}
			}
		}

		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public Object invoke(Object proxy, Method method, final Object[] args) throws Throwable {

			if (isOn(method)) {
				Selector sel = selectors.get(method);
				if (sel != null) {
					if (args.length == 0 || !(args[0] instanceof Consumer)) {
						return proxy;
					}
					reactor.on(sel, new Consumer<Event<Object>>() {
						@Override
						public void accept(Event<Object> ev) {
							// There's only two options: accept the Event or the raw value
							try {
								((Consumer) args[0]).accept(ev);
							} catch (ClassCastException ignored) {
								// If this doesn't work we're screwed anyway
								((Consumer) args[0]).accept(ev.getData());
							}
						}
					});
				}
			} else if (isNotify(method)) {
				Object key = notificationKeys.get(method);
				if (key != null) {
					if (args.length == 0) {
						reactor.notify(key);
					} else {
						reactor.notify(key, Fn.event(args[0]));
					}
				}
			}

			return proxy;
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

}
