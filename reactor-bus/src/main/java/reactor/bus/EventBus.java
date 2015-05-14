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

package reactor.bus;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Environment;
import reactor.bus.filter.PassThroughFilter;
import reactor.bus.publisher.BusPublisher;
import reactor.bus.registry.CachingRegistry;
import reactor.bus.registry.Registration;
import reactor.bus.registry.Registries;
import reactor.bus.registry.Registry;
import reactor.bus.routing.ConsumerFilteringRouter;
import reactor.bus.routing.Router;
import reactor.bus.selector.ClassSelector;
import reactor.bus.selector.Selector;
import reactor.bus.selector.Selectors;
import reactor.bus.spec.EventBusSpec;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.support.Assert;
import reactor.core.support.UUIDUtils;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.fn.support.SingleUseConsumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.UUID;

/**
 * A reactor is an event gateway that allows other components to register {@link Event} {@link Consumer}s that can
 * subsequently be notified of events. A consumer is typically registered with a {@link Selector} which, by matching on
 * the notification key, governs which events the consumer will receive. </p> When a {@literal Reactor} is notified of
 * an {@link Event}, a task is dispatched using the reactor's {@link Dispatcher} which causes it to be executed on a
 * thread based on the implementation of the {@link Dispatcher} being used.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class EventBus implements Bus<Event<?>>, Consumer<Event<?>> {

	private static final Router DEFAULT_EVENT_ROUTER = new ConsumerFilteringRouter(
			new PassThroughFilter()
	);

	private final Dispatcher                             dispatcher;
	private final Registry<Object, Consumer<? extends Event<?>>> consumerRegistry;
	private final Router                                 router;
	private final Consumer<Throwable>                    dispatchErrorHandler;
	private final Consumer<Throwable>                    uncaughtErrorHandler;

	private volatile UUID id;


	/**
	 * Create a new {@link reactor.bus.spec.EventBusSpec} to configure a Reactor.
	 *
	 * @return The Reactor spec
	 */
	public static EventBusSpec config() {
		return new EventBusSpec();
	}

	/**
	 * Create a new synchronous {@link EventBus}
	 *
	 * @return A new {@link EventBus}
	 */
	public static EventBus create() {
		return new EventBus(SynchronousDispatcher.INSTANCE);
	}

	/**
	 * Create a new {@link EventBus} using the given {@link reactor.Environment}.
	 *
	 * @param env The {@link reactor.Environment} to use.
	 * @return A new {@link EventBus}
	 */
	public static EventBus create(Environment env) {
		return new EventBusSpec().env(env).dispatcher(env.getDefaultDispatcher()).get();
	}


	/**
	 * Create a new {@link EventBus} using the given {@link reactor.core.Dispatcher}.
	 *
	 * @param dispatcher The name of the {@link reactor.core.Dispatcher} to use.
	 * @return A new {@link EventBus}
	 */
	public static EventBus create(Dispatcher dispatcher) {
		return new EventBusSpec().dispatcher(dispatcher).get();
	}

	/**
	 * Create a new {@link EventBus} using the given {@link reactor.Environment} and dispatcher name.
	 *
	 * @param env        The {@link reactor.Environment} to use.
	 * @param dispatcher The name of the {@link reactor.core.Dispatcher} to use.
	 * @return A new {@link EventBus}
	 */
	public static EventBus create(Environment env, String dispatcher) {
		return new EventBusSpec().env(env).dispatcher(dispatcher).get();
	}

	/**
	 * Create a new {@link EventBus} using the given {@link reactor.Environment} and {@link
	 * reactor.core.Dispatcher}.
	 *
	 * @param env        The {@link reactor.Environment} to use.
	 * @param dispatcher The {@link reactor.core.Dispatcher} to use.
	 * @return A new {@link EventBus}
	 */
	public static EventBus create(Environment env, Dispatcher dispatcher) {
		return new EventBusSpec().env(env).dispatcher(dispatcher).get();
	}



	/**
	 * Create a new {@literal Reactor} that uses the given {@link Dispatcher}. The reactor will use a default {@link
	 * reactor.bus.routing.Router} that broadcast events to all of the registered consumers that {@link
	 * Selector#matches(Object) match}
	 * the notification key and does not perform any type conversion.
	 *
	 * @param dispatcher The {@link Dispatcher} to use. May be {@code null} in which case a new {@link
	 *                   SynchronousDispatcher} is used
	 */
	public EventBus(@Nullable Dispatcher dispatcher) {
		this(dispatcher, null);
	}

	/**
	 * Create a new {@literal Reactor} that uses the given {@link Dispatcher}. The reactor will use a default {@link
	 * CachingRegistry}.
	 *
	 * @param dispatcher The {@link Dispatcher} to use. May be {@code null} in which case a new synchronous  dispatcher
	 *                   is used.
	 * @param router     The {@link Router} used to route events to {@link Consumer Consumers}. May be {@code null} in
	 *                   which case the
	 *                   default event router that broadcasts events to all of the registered consumers that {@link
	 *                   Selector#matches(Object) match} the notification key and does not perform any type conversion
	 *                   will be used.
	 */
	public EventBus(@Nullable Dispatcher dispatcher,
	                @Nullable Router router) {
		this(dispatcher, router, null, null);
	}

	public EventBus(@Nullable Dispatcher dispatcher,
	                @Nullable Router router,
	                @Nullable Consumer<Throwable> dispatchErrorHandler,
	                @Nullable final Consumer<Throwable> uncaughtErrorHandler) {
		this(Registries.<Object, Consumer<? extends Event<?>>>create(),
				dispatcher,
				router,
				dispatchErrorHandler,
				uncaughtErrorHandler);
	}

	/**
	 * Create a new {@literal Reactor} that uses the given {@code dispatacher} and {@code eventRouter}.
	 *
	 * @param dispatcher       The {@link Dispatcher} to use. May be {@code null} in which case a new synchronous
	 *                         dispatcher is used.
	 * @param router           The {@link Router} used to route events to {@link Consumer Consumers}. May be {@code
	 *                         null} in which case the
	 *                         default event router that broadcasts events to all of the registered consumers that {@link
	 *                         Selector#matches(Object) match} the notification key and does not perform any type
	 *                         conversion will be used.
	 * @param consumerRegistry The {@link Registry} to be used to match {@link Selector} and dispatch to {@link
	 *                         Consumer}.
	 */
	public EventBus(@Nonnull Registry<Object, Consumer<? extends Event<?>>> consumerRegistry,
	                @Nullable Dispatcher dispatcher,
	                @Nullable Router router,
	                @Nullable Consumer<Throwable> dispatchErrorHandler,
	                @Nullable final Consumer<Throwable> uncaughtErrorHandler) {
		Assert.notNull(consumerRegistry, "Consumer Registry cannot be null.");
		this.consumerRegistry = consumerRegistry;
		this.dispatcher = (null == dispatcher ? SynchronousDispatcher.INSTANCE : dispatcher);
		this.router = (null == router ? DEFAULT_EVENT_ROUTER : router);
		if (null == dispatchErrorHandler) {
			this.dispatchErrorHandler = new Consumer<Throwable>() {
				@Override
				public void accept(Throwable t) {
					Class<? extends Throwable> type = t.getClass();
					EventBus.this.router.route(type,
							Event.wrap(t),
							EventBus.this.consumerRegistry.select(type),
							null,
							null);
				}
			};
		} else {
			this.dispatchErrorHandler = dispatchErrorHandler;
		}

		this.uncaughtErrorHandler = uncaughtErrorHandler;

		this.on(new ClassSelector(Throwable.class), new Consumer<Event<Throwable>>() {
			Logger log;

			@Override
			public void accept(Event<Throwable> ev) {
				if (null == uncaughtErrorHandler) {
					if (null == log) {
						log = LoggerFactory.getLogger(EventBus.class);
					}
					log.error(ev.getData().getMessage(), ev.getData());
				} else {
					uncaughtErrorHandler.accept(ev.getData());
				}
			}
		});
	}

	/**
	 * Get the unique, time-used {@link UUID} of this {@literal Reactor}.
	 *
	 * @return The {@link UUID} of this {@literal Reactor}.
	 */
	public synchronized UUID getId() {
		if (null == id) {
			id = UUIDUtils.create();
		}
		return id;
	}

	/**
	 * Get the {@link Registry} is use to maintain the {@link Consumer}s currently listening for events on this {@literal
	 * Reactor}.
	 *
	 * @return The {@link Registry} in use.
	 */
	public Registry<?, Consumer<? extends Event<?>>> getConsumerRegistry() {
		return consumerRegistry;
	}

	/**
	 * Get the {@link Dispatcher} currently in use.
	 *
	 * @return The {@link Dispatcher}.
	 */
	public Dispatcher getDispatcher() {
		return dispatcher;
	}

	/**
	 * Get the {@link reactor.bus.routing.Router} used to route events to {@link Consumer Consumers}.
	 *
	 * @return The {@link reactor.bus.routing.Router}.
	 */
	public Router getRouter() {
		return router;
	}

	public Consumer<Throwable> getDispatchErrorHandler() {
		return dispatchErrorHandler;
	}

	public Consumer<Throwable> getUncaughtErrorHandler() {
		return uncaughtErrorHandler;
	}

	@Override
	public boolean respondsToKey(Object key) {
		List<Registration<Object, ? extends Consumer<? extends Event<?>>>> registrations = consumerRegistry.select(key);
		if (registrations.isEmpty()) return false;

		for (Registration<?, ?> reg : registrations) {
			if (!reg.isCancelled()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public <T extends Event<?>> Registration<Object, Consumer<? extends Event<?>>> on(final Selector selector,
	                                                         final Consumer<T> consumer) {
		Assert.notNull(selector, "Selector cannot be null.");
		Assert.notNull(consumer, "Consumer cannot be null.");

		final Class<?> tClass = extractGeneric(consumer);

		Consumer<T> proxyConsumer = new Consumer<T>() {
				@Override
				public void accept(T e) {
					if (null != selector.getHeaderResolver()) {
						e.getHeaders().setAll(selector.getHeaderResolver().resolve(e.getKey()));
					}
					if (tClass == null || e.getData() == null || tClass.isAssignableFrom(e.getData().getClass())) {
						consumer.accept(e);
					}
				}
			};

		return consumerRegistry.register(selector, proxyConsumer);
	}

	private Class<?> extractGeneric(Consumer<? extends Event<?>> consumer) {
		if(consumer.getClass().getGenericInterfaces().length == 0) return null;

		Type t = consumer.getClass().getGenericInterfaces()[0];
		if (ParameterizedType.class.isAssignableFrom(t.getClass())) {
			ParameterizedType pt = (ParameterizedType) t;

			if(pt.getActualTypeArguments().length == 0) return null;

			t = pt.getActualTypeArguments()[0];
			if (ParameterizedType.class.isAssignableFrom(t.getClass())) {
				pt = (ParameterizedType) t;

				if(pt.getActualTypeArguments().length == 0) return null;

				Type t1 = pt.getActualTypeArguments()[0];
				if (t1 instanceof ParameterizedType) {
					return (Class<?>) ((ParameterizedType) t1).getRawType();
				} else if (t1 instanceof Class) {
					return (Class<?>) t1;
				}
			}
		}
		return null;
	}


	/**
	 * Attach a Publisher to the {@link Bus} with the specified {@link Selector}.
	 *
	 * @param broadcastSelector the {@link Selector}/{@literal Object} tuple to listen to
	 * @return a new {@link Publisher}
	 * @since 2.0
	 */
	public Publisher<? extends Event<?>> on(Selector broadcastSelector) {
		return new BusPublisher<>(this, broadcastSelector);
	}

	@Override
	public EventBus notify(Object key, Event<?> ev) {
		Assert.notNull(key, "Key cannot be null.");
		Assert.notNull(ev, "Event cannot be null.");
		ev.setKey(key);
		dispatcher.dispatch(ev, this, dispatchErrorHandler);

		return this;
	}

	/**
	 * Pass values accepted by this {@code Stream} into the given {@link Bus}, notifying with the given key.
	 *
	 * @param key        the key to notify on
	 * @param source the {@link Publisher} to consume
	 * @return {@literal new Stream}
	 * @since 1.1, 2.0
	 */
	public final EventBus notify(@Nonnull final Publisher<?> source, @Nonnull final Object key) {
		return notify(source, new Function<Object,Object>(){
			@Override
			public Object apply(Object o) {
				return key;
			}
		});
	}

	/**
	 * Pass values accepted by this {@code Stream} into the given {@link Bus}, notifying with the given key.
	 *
	 * @param source the {@link Publisher} to consume
	 * @param keyMapper  the key function mapping each incoming data to a key to notify on
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final <T> EventBus notify(@Nonnull final Publisher<? extends T> source, @Nonnull final Function<? super T, ?> keyMapper) {
		source.subscribe(new Subscriber<T>() {
			Subscription s;
			@Override
			public void onSubscribe(Subscription s) {
				this.s = s;
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(T t) {
				EventBus.this.notify(keyMapper.apply(t), Event.wrap(t));
			}

			@Override
			public void onError(Throwable t) {
				if(s != null) s.cancel();
			}

			@Override
			public void onComplete() {
				if(s != null) s.cancel();
			}
		});
		return this;
	}

	/**
	 * Assign a {@link reactor.fn.Function} to receive an {@link Event} and produce a reply of the given type.
	 *
	 * @param sel The {@link Selector} to be used for matching
	 * @param fn  The transformative {@link reactor.fn.Function} to call to receive an {@link Event}
	 * @return A {@link Registration} object that allows the caller to interact with the given mapping
	 */
	public <T extends Event<?>, V> Registration<?, Consumer<? extends Event<?>>> receive(Selector sel, Function<T, V> fn) {
		return on(sel, new ReplyToConsumer<>(fn));
	}

	/**
	 * Notify this component that the given {@link reactor.fn.Supplier} can provide an event that's ready to be
	 * processed.
	 *
	 * @param key      The key to be matched by {@link Selector Selectors}
	 * @param supplier The {@link reactor.fn.Supplier} that will provide the actual {@link Event}
	 * @return {@literal this}
	 */
	public EventBus notify(Object key, Supplier<? extends Event<?>> supplier) {
		return notify(key, supplier.get());
	}

	/**
	 * Notify this component that the consumers registered with a {@link Selector} that matches the {@code key} should be
	 * triggered with a {@literal null} input argument.
	 *
	 * @param key The key to be matched by {@link Selector Selectors}
	 * @return {@literal this}
	 */
	public EventBus notify(Object key) {
		return notify(key, new Event<>(Void.class));
	}

	/**
	 * Notify this component of the given {@link Event} and register an internal {@link Consumer} that will take the
	 * output of a previously-registered {@link Function} and respond using the key set on the {@link Event}'s {@literal
	 * replyTo} property.
	 *
	 * @param key The key to be matched by {@link Selector Selectors}
	 * @param ev  The {@literal Event}
	 * @return {@literal this}
	 */
	public EventBus send(Object key, Event<?> ev) {
		return notify(key, new ReplyToEvent(ev, this));
	}


	/**
	 * Notify this component that the given {@link Supplier} will provide an {@link Event} and register an internal
	 * {@link
	 * Consumer} that will take the output of a previously-registered {@link Function} and respond using the key set on
	 * the {@link Event}'s {@literal replyTo} property.
	 *
	 * @param key      The key to be matched by {@link Selector Selectors}
	 * @param supplier The {@link Supplier} that will provide the actual {@link Event} instance
	 * @return {@literal this}
	 */
	public EventBus send(Object key, Supplier<? extends Event<?>> supplier) {
		return notify(key, new ReplyToEvent(supplier.get(), this));
	}

	/**
	 * Notify this component of the given {@link Event} and register an internal {@link Consumer} that will take the
	 * output of a previously-registered {@link Function} and respond to the key set on the {@link Event}'s {@literal
	 * replyTo} property and will call the {@code notify} method on the given {@link Bus}.
	 *
	 * @param key     The key to be matched by {@link Selector Selectors}
	 * @param ev      The {@literal Event}
	 * @param replyTo The {@link Bus} on which to invoke the notify method
	 * @return {@literal this}
	 */
	public EventBus send(Object key, Event<?> ev, Bus replyTo) {
		return notify(key, new ReplyToEvent(ev, replyTo));
	}


	/**
	 * Notify this component that the given {@link Supplier} will provide an {@link Event} and register an internal
	 * {@link
	 * Consumer} that will take the output of a previously-registered {@link Function} and respond to the key set on the
	 * {@link Event}'s {@literal replyTo} property and will call the {@code notify} method on the given {@link
	 * Bus}.
	 *
	 * @param key      The key to be matched by {@link Selector Selectors}
	 * @param supplier The {@link Supplier} that will provide the actual {@link Event} instance
	 * @param replyTo  The {@link Bus} on which to invoke the notify method
	 * @return {@literal this}
	 */
	public EventBus send(Object key, Supplier<? extends Event<?>> supplier, Bus replyTo) {
		return notify(key, new ReplyToEvent(supplier.get(), replyTo));
	}

	/**
	 * Register the given {@link reactor.fn.Consumer} on an anonymous {@link reactor.bus.selector.Selector} and
	 * set the given event's {@code replyTo} property to the corresponding anonymous key, then register the consumer to
	 * receive replies from the {@link reactor.fn.Function} assigned to handle the given key.
	 *
	 * @param key   The key to be matched by {@link Selector Selectors}
	 * @param event The event to notify.
	 * @param reply The consumer to register as a reply handler.
	 * @return {@literal this}
	 */
	public <T extends Event<?>> EventBus sendAndReceive(Object key, Event<?> event, Consumer<T> reply) {
		Selector sel = Selectors.anonymous();
		on(sel, new SingleUseConsumer<T>(reply)).cancelAfterUse();
		notify(key, event.setReplyTo(sel.getObject()));
		return this;
	}

	/**
	 * Register the given {@link reactor.fn.Consumer} on an anonymous {@link reactor.bus.selector.Selector} and
	 * set the event's {@code replyTo} property to the corresponding anonymous key, then register the consumer to receive
	 * replies from the {@link reactor.fn.Function} assigned to handle the given key.
	 *
	 * @param key      The key to be matched by {@link Selector Selectors}
	 * @param supplier The supplier to supply the event.
	 * @param reply    The consumer to register as a reply handler.
	 * @return {@literal this}
	 */
	public <T extends Event<?>> EventBus sendAndReceive(Object key, Supplier<? extends Event<?>> supplier, Consumer<T> reply) {
		return sendAndReceive(key, supplier.get(), reply);
	}

	/**
	 * Create an optimized path for publishing notifications to the given key.
	 *
	 * @param key The key to be matched by {@link Selector Selectors}
	 * @return a {@link Consumer} to invoke with the {@link Event Events} to publish
	 */
	public <T> Consumer<Event<T>> prepare(final Object key) {
		return new Consumer<Event<T>>() {
			final List<Registration<Object, ? extends Consumer<? extends Event<?>>>> regs = consumerRegistry.select(key);
			final int size = regs.size();

			@Override
			public void accept(Event<T> ev) {
				for (int i = 0; i < size; i++) {
					Registration<Object, ? extends Consumer<Event<T>>> reg =
							(Registration<Object, ? extends Consumer<Event<T>>>) regs.get(i);
					ev.setKey(key);
					dispatcher.dispatch(ev, reg.getObject(), dispatchErrorHandler);
				}
			}
		};
	}

	/**
	 * Schedule an arbitrary {@link reactor.fn.Consumer} to be executed on the current Reactor  {@link
	 * reactor.core.Dispatcher}, passing the given {@param data}.
	 *
	 * @param consumer The {@link reactor.fn.Consumer} to invoke.
	 * @param data     The data to pass to the consumer.
	 * @param <T>      The type of the data.
	 */
	public <T> void schedule(final Consumer<T> consumer, final T data) {
		dispatcher.dispatch(null, new Consumer<Event<?>>() {
			@Override
			public void accept(Event<?> event) {
				consumer.accept(data);
			}
		}, dispatchErrorHandler);
	}

	@Override
	public void accept(Event<?> event) {
		router.route(event.getKey(), event, consumerRegistry.select(event.getKey()), null, dispatchErrorHandler);
	}

	public static class ReplyToEvent<T> extends Event<T> {
		private static final long serialVersionUID = 1937884784799135647L;
		private final Bus replyToObservable;

		private ReplyToEvent(Headers headers, T data, Object replyTo,
		                     Bus replyToObservable,
		                     Consumer<Throwable> errorConsumer) {
			super(headers, data, errorConsumer);
			setReplyTo(replyTo);
			this.replyToObservable = replyToObservable;
		}

		private ReplyToEvent(Event<T> delegate, Bus replyToObservable) {
			this(delegate.getHeaders(), delegate.getData(), delegate.getReplyTo(), replyToObservable,
					delegate.getErrorConsumer());
		}

		@Override
		public <X> Event<X> copy(X data) {
			return new ReplyToEvent<X>(getHeaders(), data, getReplyTo(), replyToObservable, getErrorConsumer());
		}

		public Bus getReplyToObservable() {
			return replyToObservable;
		}
	}

	public class ReplyToConsumer<E extends Event<?>, V> implements Consumer<E> {
		private final Function<E, V> fn;

		private ReplyToConsumer(Function<E, V> fn) {
			this.fn = fn;
		}

		@Override
		public void accept(E ev) {
			Bus replyToObservable = EventBus.this;

			if (ReplyToEvent.class.isAssignableFrom(ev.getClass())) {
				Bus o = ((ReplyToEvent<?>) ev).getReplyToObservable();
				if (null != o) {
					replyToObservable = o;
				}
			}

			try {
				V reply = fn.apply(ev);

				Event<?> replyEv;
				if (null == reply) {
					replyEv = new Event<Void>(Void.class);
				} else {
					replyEv = (Event.class.isAssignableFrom(reply.getClass()) ? (Event<?>) reply : Event.wrap(reply));
				}

				replyToObservable.notify(ev.getReplyTo(), replyEv);
			} catch (Throwable x) {
				replyToObservable.notify(x.getClass(), Event.wrap(x));
			}
		}

		public Function<E, V> getDelegate() {
			return fn;
		}
	}

}
