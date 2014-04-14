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

package reactor.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.registry.CachingRegistry;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.routing.ArgumentConvertingConsumerInvoker;
import reactor.event.routing.ConsumerFilteringRouter;
import reactor.event.routing.Router;
import reactor.event.selector.ClassSelector;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.filter.PassThroughFilter;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Supplier;
import reactor.function.support.SingleUseConsumer;
import reactor.util.Assert;
import reactor.util.UUIDUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
public class Reactor implements Observable {

	private static final Router DEFAULT_EVENT_ROUTER = new ConsumerFilteringRouter(
			new PassThroughFilter(), new ArgumentConvertingConsumerInvoker(null)
	);

	private final Dispatcher            dispatcher;
	private final Registry<Consumer<?>> consumerRegistry;
	private final Router                router;
	private final Consumer<Throwable>   dispatchErrorHandler;

	private volatile UUID id;

	/**
	 * Create a new {@literal Reactor} that uses the given {@link Dispatcher}. The reactor will use a default {@link
	 * reactor.event.routing.Router} that broadcast events to all of the registered consumers that {@link Selector#matches(Object) match}
	 * the notification key and does not perform any type conversion.
	 *
	 * @param dispatcher
	 * 		The {@link Dispatcher} to use. May be {@code null} in which case a new {@link SynchronousDispatcher} is used
	 */
	public Reactor(@Nullable Dispatcher dispatcher) {
		this(dispatcher, null);
	}

	/**
	 * Create a new {@literal Reactor} that uses the given {@link Dispatcher}. The reactor will use a default {@link
	 * CachingRegistry}.
	 *
	 * @param dispatcher
	 * 		The {@link Dispatcher} to use. May be {@code null} in which case a new synchronous  dispatcher is used.
	 * @param router
	 * 		The {@link Router} used to route events to {@link Consumer Consumers}. May be {@code null} in which case the
	 * 		default event router that broadcasts events to all of the registered consumers that {@link
	 * 		Selector#matches(Object) match} the notification key and does not perform any type conversion will be used.
	 */
	public Reactor(@Nullable Dispatcher dispatcher,
	               @Nullable Router router) {
		this(dispatcher, router, null, null);
	}

	public Reactor(@Nullable Dispatcher dispatcher,
	               @Nullable Router router,
	               @Nullable Consumer<Throwable> dispatchErrorHandler,
	               @Nullable final Consumer<Throwable> uncaughtErrorHandler) {
		this(new CachingRegistry<Consumer<?>>(),
				dispatcher,
				router,
				dispatchErrorHandler,
				uncaughtErrorHandler);
	}

	/**
	 * Create a new {@literal Reactor} that uses the given {@code dispatacher} and {@code eventRouter}.
	 *
	 * @param dispatcher
	 * 		The {@link Dispatcher} to use. May be {@code null} in which case a new synchronous  dispatcher is used.
	 * @param router
	 * 		The {@link Router} used to route events to {@link Consumer Consumers}. May be {@code null} in which case the
	 * 		default event router that broadcasts events to all of the registered consumers that {@link
	 * 		Selector#matches(Object) match} the notification key and does not perform any type conversion will be used.
	 * @param consumerRegistry
	 * 		The {@link Registry} to be used to match {@link Selector} and dispatch to {@link Consumer}.
	 */
	public Reactor(@Nonnull Registry<Consumer<?>> consumerRegistry,
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
					Reactor.this.router.route(type,
							Event.wrap(t),
							Reactor.this.consumerRegistry.select(type),
							null,
							null);
				}
			};
		} else {
			this.dispatchErrorHandler = dispatchErrorHandler;
		}

		this.on(new ClassSelector(Throwable.class), new Consumer<Event<Throwable>>() {
			Logger log;

			@Override
			public void accept(Event<Throwable> ev) {
				if (null == uncaughtErrorHandler) {
					if (null == log) {
						log = LoggerFactory.getLogger(Reactor.class);
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
	public Registry<Consumer<?>> getConsumerRegistry() {
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
	 * Get the {@link reactor.event.routing.Router} used to route events to {@link Consumer Consumers}.
	 *
	 * @return The {@link reactor.event.routing.Router}.
	 */
	public Router getRouter() {
		return router;
	}


	@Override
	public boolean respondsToKey(Object key) {
		for (Registration<?> reg : consumerRegistry.select(key)) {
			if (!reg.isCancelled()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public <E extends Event<?>> Registration<Consumer<E>> on(final Selector selector, final Consumer<E> consumer) {
		Assert.notNull(selector, "Selector cannot be null.");
		Assert.notNull(consumer, "Consumer cannot be null.");
		if (null != selector.getHeaderResolver()) {
			Consumer<E> proxyConsumer = new Consumer<E>() {
				@Override
				public void accept(E e) {
					e.getHeaders().setAll(selector.getHeaderResolver().resolve(e.getKey()));
					consumer.accept(e);
				}
			};
			return consumerRegistry.register(selector, proxyConsumer);
		}else{
			return consumerRegistry.register(selector, consumer);
		}
	}

	@Override
	public <E extends Event<?>, V> Registration<Consumer<E>> receive(Selector sel, Function<E, V> fn) {
		return on(sel, new ReplyToConsumer<E, V>(fn));
	}

	@Override
	public <E extends Event<?>> Reactor notify(Object key, E ev, Consumer<E> onComplete) {
		Assert.notNull(key, "Key cannot be null.");
		Assert.notNull(ev, "Event cannot be null.");
		ev.setKey(key);
		dispatcher.dispatch(key, ev, consumerRegistry, dispatchErrorHandler, router, onComplete);

		return this;
	}

	@Override
	public <E extends Event<?>> Reactor notify(Object key, E ev) {
		return notify(key, ev, null);
	}

	@Override
	public <S extends Supplier<? extends Event<?>>> Reactor notify(Object key, S supplier) {
		return notify(key, supplier.get(), null);
	}

	@Override
	public Reactor notify(Object key) {
		return notify(key, new Event<Void>(Void.class), null);
	}

	@Override
	public <E extends Event<?>> Reactor send(Object key, E ev) {
		return notify(key, new ReplyToEvent(ev, this));
	}

	@Override
	public <S extends Supplier<? extends Event<?>>> Reactor send(Object key, S supplier) {
		return notify(key, new ReplyToEvent(supplier.get(), this));
	}

	@Override
	public <E extends Event<?>> Reactor send(Object key, E ev, Observable replyTo) {
		return notify(key, new ReplyToEvent(ev, replyTo));
	}

	@Override
	public <S extends Supplier<? extends Event<?>>> Reactor send(Object key, S supplier, Observable replyTo) {
		return notify(key, new ReplyToEvent(supplier.get(), replyTo));
	}

	@Override
	public <REQ extends Event<?>, RESP extends Event<?>> Reactor sendAndReceive(Object key,
	                                                                            REQ ev,
	                                                                            Consumer<RESP> reply) {
		Selector sel = Selectors.anonymous();
		on(sel, new SingleUseConsumer<RESP>(reply)).cancelAfterUse();
		notify(key, ev.setReplyTo(sel.getObject()));
		return this;
	}

	@Override
	public <REQ extends Event<?>, RESP extends Event<?>, S extends Supplier<REQ>> Reactor sendAndReceive(Object key,
	                                                                                                     S supplier,
	                                                                                                     Consumer<RESP> reply) {
		return sendAndReceive(key, supplier.get(), reply);
	}

	@Override
	public <T> Consumer<Event<T>> prepare(final Object key) {
		return new Consumer<Event<T>>() {
			final List<Registration<? extends Consumer<?>>> regs = consumerRegistry.select(key);
			final int size = regs.size();

			@Override
			public void accept(Event<T> ev) {
				for (int i = 0; i < size; i++) {
					Registration<Consumer<Event<?>>> reg = (Registration<Consumer<Event<?>>>) regs.get(i);
					dispatcher.dispatch(ev.setKey(key), router, reg.getObject(), dispatchErrorHandler);
				}
			}
		};
	}

	@Override
	public <T> Consumer<Iterable<Event<T>>> batchNotify(final Object key) {
		return batchNotify(key, null);
	}

	@Override
	public <T> Consumer<Iterable<Event<T>>> batchNotify(final Object key, final Consumer<Void> completeConsumer) {
		return new Consumer<Iterable<Event<T>>>() {
			final Consumer<Iterable<Event<T>>> batchConsumer = new Consumer<Iterable<Event<T>>>() {
				@Override
				public void accept(Iterable<Event<T>> event) {
					List<Registration<? extends Consumer<?>>> regs = consumerRegistry.select(key);
					for (Event<T> batchedEvent : event) {
						for (Registration<? extends Consumer<?>> registration : regs) {
							router.route(null, batchedEvent, null, registration.getObject(), dispatchErrorHandler);
						}
					}
					if (completeConsumer != null) {
						completeConsumer.accept(null);
					}
				}
			};

			@Override
			public void accept(Iterable<Event<T>> evs) {
				dispatcher.dispatch(null, evs, null, dispatchErrorHandler, router, batchConsumer);
			}
		};
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		return hashCode() == o.hashCode();

	}

	/**
	 * Schedule an arbitrary {@link reactor.function.Consumer} to be executed on the current Reactor  {@link
	 * reactor.event.dispatch.Dispatcher}, passing the given {@param data}.
	 *
	 * @param consumer
	 * 		The {@link reactor.function.Consumer} to invoke.
	 * @param data
	 * 		The data to pass to the consumer.
	 * @param <T>
	 * 		The type of the data.
	 */
	public <T> void schedule(final Consumer<T> consumer, final T data) {
		dispatcher.dispatch(null, null, null, dispatchErrorHandler, router, new Consumer<Event<?>>() {
			@Override
			public void accept(Event<?> event) {
				consumer.accept(data);
			}
		});
	}

	public static class ReplyToEvent<T> extends Event<T> {
		private static final long serialVersionUID = 1937884784799135647L;
		private final Observable replyToObservable;

		private ReplyToEvent(Headers headers, T data, Object replyTo,
		                     Observable replyToObservable,
		                     Consumer<Throwable> errorConsumer) {
			super(headers, data, errorConsumer);
			setReplyTo(replyTo);
			this.replyToObservable = replyToObservable;
		}

		private ReplyToEvent(Event<T> delegate, Observable replyToObservable) {
			this(delegate.getHeaders(), delegate.getData(), delegate.getReplyTo(), replyToObservable,
					delegate.getErrorConsumer());
		}

		@Override
		public <X> Event<X> copy(X data) {
			return new ReplyToEvent<X>(getHeaders(), data, getReplyTo(), replyToObservable, getErrorConsumer());
		}

		public Observable getReplyToObservable() {
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
			Observable replyToObservable = Reactor.this;

			if (ReplyToEvent.class.isAssignableFrom(ev.getClass())) {
				Observable o = ((ReplyToEvent<?>) ev).getReplyToObservable();
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
