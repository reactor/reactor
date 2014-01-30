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
import reactor.event.routing.ConsumerFilteringEventRouter;
import reactor.event.routing.EventRouter;
import reactor.event.selector.ClassSelector;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.filter.PassThroughFilter;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Supplier;
import reactor.function.support.SingleUseConsumer;
import reactor.tuple.Tuple2;
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

	private static final EventRouter DEFAULT_EVENT_ROUTER = new ConsumerFilteringEventRouter(
			new PassThroughFilter(), new ArgumentConvertingConsumerInvoker(null)
	);

	private final Dispatcher                             dispatcher;
	private final Registry<Consumer<? extends Event<?>>> consumerRegistry;
	private final EventRouter                            eventRouter;
	private final Selector                               defaultSelector;
	private final Object                                 defaultKey;
	private final Consumer<Throwable>                    dispatchErrorHandler;

	private volatile UUID id;

	/**
	 * Create a new {@literal Reactor} that uses the given {@link Dispatcher}. The reactor will use a default {@link
	 * EventRouter} that broadcast events to all of the registered consumers that {@link Selector#matches(Object) match}
	 * the notification key and does not perform any type conversion.
	 *
	 * @param dispatcher
	 * 		The {@link Dispatcher} to use. May be {@code null} in which case a new {@link
	 * 		SynchronousDispatcher} is used
	 */
	public Reactor(@Nullable Dispatcher dispatcher) {
		this(dispatcher, null);
	}

	/**
	 * Create a new {@literal Reactor} that uses the given {@link Dispatcher}. The reactor will use a default {@link
	 * CachingRegistry}.
	 *
	 * @param dispatcher
	 * 		The {@link Dispatcher} to use. May be {@code null} in which case a new synchronous  dispatcher is
	 * 		used.
	 * @param eventRouter
	 * 		The {@link EventRouter} used to route events to {@link Consumer Consumers}. May be {@code null}
	 * 		in which case the default event router that broadcasts events to all of the registered consumers
	 * 		that {@link Selector#matches(Object) match} the notification key and does not perform any type
	 * 		conversion will be used.
	 */
	public Reactor(@Nullable Dispatcher dispatcher,
	               @Nullable EventRouter eventRouter) {
		this(dispatcher, eventRouter, null, null, null);
	}

	public Reactor(@Nullable Dispatcher dispatcher,
	               @Nullable EventRouter eventRouter,
	               @Nullable Selector defaultSelector,
	               @Nullable Consumer<Throwable> dispatchErrorHandler,
	               @Nullable final Consumer<Throwable> uncaughtErrorHandler) {
		this(new CachingRegistry<Consumer<? extends Event<?>>>(),
		     dispatcher,
		     eventRouter,
		     defaultSelector,
		     dispatchErrorHandler,
		     uncaughtErrorHandler);
	}

	/**
	 * Create a new {@literal Reactor} that uses the given {@code dispatacher} and {@code eventRouter}.
	 *
	 * @param dispatcher
	 * 		The {@link Dispatcher} to use. May be {@code null} in which case a new synchronous  dispatcher is
	 * 		used.
	 * @param eventRouter
	 * 		The {@link EventRouter} used to route events to {@link Consumer Consumers}. May be {@code null}
	 * 		in which case the default event router that broadcasts events to all of the registered consumers
	 * 		that {@link Selector#matches(Object) match} the notification key and does not perform any type
	 * 		conversion will be used.
	 * @param consumerRegistry
	 * 		The {@link Registry} to be used to match {@link Selector} and dispatch to {@link Consumer}.
	 */
	public Reactor(@Nonnull Registry<Consumer<? extends Event<?>>> consumerRegistry,
	               @Nullable Dispatcher dispatcher,
	               @Nullable EventRouter eventRouter,
	               @Nullable Selector defaultSelector,
	               @Nullable Consumer<Throwable> dispatchErrorHandler,
	               @Nullable final Consumer<Throwable> uncaughtErrorHandler) {
		Assert.notNull(consumerRegistry, "Consumer Registry cannot be null.");
		this.consumerRegistry = consumerRegistry;
		this.dispatcher = (null == dispatcher ? new SynchronousDispatcher() : dispatcher);
		this.eventRouter = (null == eventRouter ? DEFAULT_EVENT_ROUTER : eventRouter);
		this.defaultSelector = (null == defaultSelector ? Selectors.anonymous() : defaultSelector);
		this.defaultKey = this.defaultSelector.getObject();
		if(null == dispatchErrorHandler) {
			this.dispatchErrorHandler = new Consumer<Throwable>() {
				@Override
				public void accept(Throwable t) {
					Class<? extends Throwable> type = t.getClass();
					Reactor.this.eventRouter.route(type,
					                               Event.wrap(t).setKey(type),
					                               Reactor.this.consumerRegistry.select(type),
					                               null,
					                               null);
				}
			};
		} else {
			this.dispatchErrorHandler = dispatchErrorHandler;
		}

		// Register a special Consumer that will schedule arbitrary tasks on this Dispatcher
		this.on(new Consumer<Event>() {
			@Override
			public void accept(Event event) {
				if(Tuple2.class.isInstance(event.getData())) {
					Object consumer = ((Tuple2)event.getData()).getT1();
					Object data = ((Tuple2)event.getData()).getT2();
					if(Consumer.class.isInstance(consumer)) {
						try {
							((Consumer)consumer).accept(data);
						} catch(Throwable t) {
							Reactor.this.notify(t.getClass(), Event.wrap(t));
						}
					}
				}
			}
		});

		this.on(new ClassSelector(Throwable.class), new Consumer<Event<Throwable>>() {
			Logger log;

			@Override
			public void accept(Event<Throwable> ev) {
				if(null == uncaughtErrorHandler) {
					if(null == log) {
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
		if(null == id) {
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
	public Registry<Consumer<? extends Event<?>>> getConsumerRegistry() {
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
	 * Get the {@link EventRouter} used to route events to {@link Consumer Consumers}.
	 *
	 * @return The {@link EventRouter}.
	 */
	public EventRouter getEventRouter() {
		return eventRouter;
	}


	@Override
	public boolean respondsToKey(Object key) {
		for(Registration<?> reg : consumerRegistry.select(key)) {
			if(!reg.isCancelled()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public <E extends Event<?>> Registration<Consumer<E>> on(Selector selector, final Consumer<E> consumer) {
		Assert.notNull(selector, "Selector cannot be null.");
		Assert.notNull(consumer, "Consumer cannot be null.");
		Registration<Consumer<E>> reg = consumerRegistry.register(selector, consumer);
		return reg;
	}

	@Override
	public <E extends Event<?>> Registration<Consumer<E>> on(Consumer<E> consumer) {
		Assert.notNull(consumer, "Consumer cannot be null.");
		return on(defaultSelector, consumer);
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
		dispatcher.dispatch(key, ev, consumerRegistry, dispatchErrorHandler, eventRouter, onComplete);

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
	public <E extends Event<?>> Reactor notify(E ev) {
		return notify(defaultKey, ev, null);
	}

	@Override
	public <S extends Supplier<? extends Event<?>>> Reactor notify(S supplier) {
		return notify(defaultKey, supplier.get(), null);
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
	public <E extends Event<?>> Reactor sendAndReceive(Object key, E ev, Consumer<E> reply) {
		Selector sel = Selectors.anonymous();
		on(sel, new SingleUseConsumer<E>(reply)).cancelAfterUse();
		notify(key, ev.setReplyTo(sel.getObject()));
		return this;
	}

	@Override
	public <E extends Event<?>, S extends Supplier<E>> Reactor sendAndReceive(Object key,
	                                                                          S supplier,
	                                                                          Consumer<E> reply) {
		return sendAndReceive(key, supplier.get(), reply);
	}

	@Override
	public <T> Consumer<Event<T>> prepare(final Object key) {
		return new Consumer<Event<T>>() {
			final List<Registration<? extends Consumer<? extends Event<?>>>> regs = consumerRegistry.select(key);
			final int size = regs.size();

			@Override
			public void accept(Event<T> ev) {
				for(int i = 0; i < size; i++) {
					Registration<Consumer<Event<?>>> reg = (Registration<Consumer<Event<?>>>)regs.get(i);
					dispatcher.dispatch(ev.setKey(key), eventRouter, reg.getObject(), dispatchErrorHandler);
				}
			}
		};
	}

	public <T> Consumer<T> consumer() {
		return new Consumer<T>() {
			@Override
			public void accept(T obj) {
				Reactor.this.notify(Event.wrap(obj));
			}
		};
	}

	@Override
	public boolean equals(Object o) {
		if(this == o) {
			return true;
		}
		if(o == null || getClass() != o.getClass()) {
			return false;
		}

		return hashCode() == o.hashCode();

	}

	public static class ReplyToEvent<T> extends Event<T> {
		private static final long serialVersionUID = 1937884784799135647L;
		private final Observable replyToObservable;

		@Override
		public <X> Event<X> copy(X data) {
			return new ReplyToEvent<X>(getHeaders(), data, getReplyTo(), replyToObservable, getErrorConsumer());
		}

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

			if(ReplyToEvent.class.isAssignableFrom(ev.getClass())) {
				Observable o = ((ReplyToEvent<?>)ev).getReplyToObservable();
				if(null != o) {
					replyToObservable = o;
				}
			}

			try {
				V reply = fn.apply(ev);

				Event<?> replyEv;
				if(null == reply) {
					replyEv = new Event<Void>(Void.class);
				} else {
					replyEv = (Event.class.isAssignableFrom(reply.getClass()) ? (Event<?>)reply : Event.wrap(reply));
				}

				replyToObservable.notify(ev.getReplyTo(), replyEv);
			} catch(Throwable x) {
				replyToObservable.notify(x.getClass(), Event.wrap(x));
			}
		}

		public Function<E, V> getDelegate() {
			return fn;
		}
	}

}
