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

import org.cliffc.high_scale_lib.NonBlockingHashSet;
import reactor.convert.Converter;
import reactor.fn.*;
import reactor.fn.dispatch.Dispatcher;
import reactor.fn.dispatch.SynchronousDispatcher;
import reactor.fn.registry.CachingRegistry;
import reactor.fn.registry.Registration;
import reactor.fn.registry.Registry;
import reactor.fn.registry.SelectionStrategy;
import reactor.fn.routing.ConsumerFilteringEventRouter;
import reactor.fn.routing.EventRouter;
import reactor.fn.routing.Linkable;
import reactor.fn.selector.Selector;
import reactor.fn.tuples.Tuple2;
import reactor.util.Assert;
import reactor.util.UUIDUtils;

import java.util.Set;
import java.util.UUID;

import static reactor.fn.Functions.$;

/**
 * A reactor is an event gateway that allows other components to register {@link Event} (@link Consumer}s with its
 * {@link reactor.fn.selector.Selector ) {@link reactor.fn.registry.Registry }. When a {@literal Reactor} is notified of
 * that {@link Event}, a task is dispatched to the assigned {@link Dispatcher} which causes it to be executed on a
 * thread based on the implementation of the {@link Dispatcher} being used.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class Reactor implements Observable, Linkable<Observable> {

	private final Environment                            env;
	private final Dispatcher                             dispatcher;
	private final Registry<Consumer<? extends Event<?>>> consumerRegistry;
	private final EventRouter                            eventRouter;

	private final Object   defaultKey      = new Object();
	private final Selector defaultSelector = $(defaultKey);

	private final Object   registerKey      = new Object();
	private final Selector registerSelector = $(registerKey);

	private final UUID                id             = UUIDUtils.create();
	private final Consumer<Throwable> errorHandler   = new Consumer<Throwable>() {
		@Override
		public void accept(Throwable t) {
			//avoid passing itself as error handler
			dispatcher.dispatch(t.getClass(), Event.wrap(t), consumerRegistry, null, eventRouter, null);
		}
	};
	private final Set<Observable>     linkedReactors = new NonBlockingHashSet<Observable>();


	/**
	 * Create a new {@literal Reactor} that uses the given {@link Dispatcher}. The default {@link EventRouter}, {@link
	 * reactor.fn.registry.SelectionStrategy}, and {@link Converter} will be used.
	 *
	 * @param dispatcher The {@link Dispatcher} to use. May be {@code null} in which case a new worker dispatcher is used
	 *                   dispatcher is used
	 */
	Reactor(Environment env,
					Dispatcher dispatcher) {
		this(env,
				 dispatcher,
				 null,
				 null);
	}

	/**
	 * Create a new {@literal Reactor} that uses the given {@link Dispatcher}, {@link
	 * reactor.fn.registry.SelectionStrategy}, {@link EventRouter}
	 *
	 * @param dispatcher        The {@link Dispatcher} to use. May be {@code null} in which case a new synchronous
	 *                          dispatcher is used.
	 * @param selectionStrategy The custom {@link SelectionStrategy} to use. May be {@code null}.
	 * @param eventRouter       The {@link EventRouter} used to route events to {@link Consumer Consumers}. May be {@code
	 *                          null} in which case a default event router is used.
	 */
	Reactor(Environment env,
					Dispatcher dispatcher,
					SelectionStrategy selectionStrategy,
					EventRouter eventRouter) {
		this.env = env;
		this.dispatcher = dispatcher == null ? SynchronousDispatcher.INSTANCE : dispatcher;
		this.eventRouter = eventRouter == null ? ConsumerFilteringEventRouter.DEFAULT : eventRouter;
		this.consumerRegistry = new CachingRegistry<Consumer<? extends Event<?>>>(selectionStrategy);

		this.on(new Consumer<Event>() {
			@Override
			public void accept(Event event) {
				if (Tuple2.class.isInstance(event.getData())) {
					Object consumer = ((Tuple2) event.getData()).getT1();
					Object data = ((Tuple2) event.getData()).getT2();
					if (Consumer.class.isInstance(consumer)) {
						try {
							((Consumer) consumer).accept(data);
						} catch (Throwable t) {
							//LoggerFactory.getLogger(Reactor.class).error(t.getMessage(), t);
							Reactor.this.notify(t.getClass(), Event.wrap(t));
						}
					}
				}
			}
		});
	}

	/**
	 * Get the unique, time-used {@link UUID} of this {@literal Reactor}.
	 *
	 * @return The {@link UUID} of this {@literal Reactor}.
	 */
	public UUID getId() {
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
		Assert.notNull(key, "Key cannot be null.");
		return consumerRegistry.select(key).iterator().hasNext();
	}

	@Override
	public <E extends Event<?>> Registration<Consumer<E>> on(Selector selector, final Consumer<E> consumer) {
		Assert.notNull(selector, "Selector cannot be null.");
		Assert.notNull(consumer, "Consumer cannot be null.");
		Registration<Consumer<E>> reg = consumerRegistry.register(selector, consumer);
		notify(registerKey, Event.wrap(reg), null, ConsumerFilteringEventRouter.DEFAULT);
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

	private <E extends Event<?>> Reactor notify(Object key, E ev, Consumer<E> onComplete, EventRouter eventRouter) {
		Assert.notNull(key, "Key cannot be null.");
		Assert.notNull(ev, "Event cannot be null.");

		dispatcher.dispatch(key, ev, consumerRegistry, errorHandler, eventRouter, onComplete);

		if (!linkedReactors.isEmpty()) {
			for (Observable r : linkedReactors) {
				r.notify(key, ev);
			}
		}
		return this;
	}

	@Override
	public <E extends Event<?>> Reactor notify(Object key, E ev, Consumer<E> onComplete) {
		notify(key, ev, onComplete, eventRouter);
		return this;
	}

	@Override
	public <E extends Event<?>> Reactor notify(Object key, E ev) {
		return notify(key, ev, null, eventRouter);
	}

	@Override
	public <S extends Supplier<? extends Event<?>>> Reactor notify(Object key, S supplier) {
		return notify(key, supplier.get(), null, eventRouter);
	}

	@Override
	public <E extends Event<?>> Reactor notify(E ev) {
		return notify(defaultKey, ev, null, eventRouter);
	}

	@Override
	public <S extends Supplier<? extends Event<?>>> Reactor notify(S supplier) {
		return notify(defaultKey, supplier.get(), null, eventRouter);
	}

	@Override
	public Reactor notify(Object key) {
		return notify(key, Event.NULL_EVENT, null, eventRouter);
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


	/**
	 * Register a {@link Consumer} to be triggered when {@link #on} has been completed.
	 *
	 * @param registrationConsumer The {@literal Consumer} to be triggered.
	 * @param <E>                  The type of the event passed to the registrationConsumer.
	 * @return A {@link Registration} object that allows the caller to interact with consumer state.
	 */
	public <E extends Event<Registration<?>>> Registration<Consumer<E>> onRegistration(Consumer<E>
																																												 registrationConsumer) {
		return consumerRegistry.register(registerSelector, registrationConsumer);
	}

	/**
	 * Register a {@link Function} to be triggered using the given {@link Selector}. The return value is automatically
	 * handled and replied to the origin reactor/replyTo key set within an incoming event
	 *
	 * @param sel      The {@link Selector} to be used for matching
	 * @param function The {@literal Function} to be triggered.
	 * @param <T>      The type of the data in the {@link Event}.
	 * @param <V>      The type of the return value when the given {@link Function}.
	 * @return A {@link Stream} object that allows the caller to interact with the given mapping.
	 */
	public <T, E extends Event<T>, V> Stream<V> map(Selector sel, final Function<E, V> function) {
		Assert.notNull(sel, "Selector cannot be null.");
		Assert.notNull(function, "Function cannot be null.");

//		final Stream<V> c = Streams.<V>defer().using(env).using(this).get();
//		on(sel, new Consumer<E>() {
//			@Override
//			public void accept(E event) {
//				try {
//					c.accept(function.apply(event));
//				} catch (Throwable t) {
//					c.accept(t);
//				}
//			}
//		});
//		return c;
		return null;
	}

	/**
	 * Register a {@link Function} to be triggered using the internal, global {@link Selector} that is unique to each
	 * {@literal Observable} instance. The return value is automatically handled and replied to the origin reactor/replyTo
	 * key set within an incoming event
	 *
	 * @param function The {@literal Consumer} to be triggered.
	 * @param <T>      The type of the data in the {@link Event}.
	 * @param <V>      The type of the return value when the given {@link Function}.
	 * @return A {@link Registration} object that allows the caller to interact with the given mapping.
	 */
	public <T, E extends Event<T>, V> Stream<V> map(Function<E, V> function) {
		Assert.notNull(function, "Function cannot be null.");
		return map(defaultSelector, function);
	}

	/**
	 * Return a new composition ready to accept an event {@param ev}, thus notifying this component that the consumers
	 * matching {@param key} should consume the event and might reply using R.replyTo (which is automatically handled when
	 * used in combination with {@link #map(Function)}.
	 *
	 * @param key The notification key
	 * @param ev  The {@link Event} to publish.
	 * @return {@link Stream}
	 */
	public <T, E extends Event<T>, V> Stream<V> compose(Object key, E ev) {
//		return Streams.defer(ev).using(env).using(this).get().map(key, this);
		return null;
	}

	/**
	 * Notify this component that the consumers matching {@param key} should consume the event {@param ev} and might send
	 * replies to the consumer {@param consumer}.
	 *
	 * @param key      The notification key
	 * @param ev       The {@link Event} to publish.
	 * @param consumer The {@link Stream} to pass the replies.
	 * @return {@literal this}
	 */
	public <T, E extends Event<T>, V> Reactor compose(Object key, E ev, Consumer<V> consumer) {
//		Stream<E> composable = Streams.defer(ev).using(env).using(this).get();
//		composable.<V>map(key, this).consume(consumer);
//		composable.get();

		return this;
	}

	/**
	 * Compose an action starting with the given key and {@link Supplier}.
	 *
	 * @param key      The key to use.
	 * @param supplier The {@link Supplier} that will provide the actual {@link Event} instance.
	 * @param <T>      The type of the incoming data.
	 * @param <S>      The type of the event supplier.
	 * @param <V>      The type of the {@link Stream}.
	 * @return A new {@link Stream}.
	 */
	public <T, S extends Supplier<Event<T>>, V> Stream<V> compose(Object key, S supplier) {
		return compose(key, supplier.get());
	}


	/**
	 * Compose an action starting with the given key, the {@link Supplier} and a replies consumer.
	 *
	 * @param key      The key to use.
	 * @param supplier The {@link Supplier} that will provide the actual {@link Event} instance.
	 * @param consumer The {@link Consumer} that will consume replies.
	 * @param <T>      The type of the incoming data.
	 * @param <S>      The type of the event supplier.
	 * @param <V>      The type of the {@link Stream}.
	 * @return A new {@link Stream}.
	 */
	public <T, S extends Supplier<Event<T>>, V> Reactor compose(Object key, S supplier, Consumer<V> consumer) {
		return compose(key, supplier.get(), consumer);
	}

	@Override
	public Reactor link(Observable reactor) {
		linkedReactors.add(reactor);
		return this;
	}

	@Override
	public Reactor unlink(Observable reactor) {
		linkedReactors.remove(reactor);
		return this;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Reactor reactor = (Reactor) o;

		return id.equals(reactor.id);

	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	private static class ReplyToEvent<T> extends Event<T> {
		private final Observable replyToObservable;

		private ReplyToEvent(Event<T> delegate, Observable replyToObservable) {
			super(delegate.getHeaders(), delegate.getData());
			setReplyTo(delegate.getReplyTo());
			this.replyToObservable = replyToObservable;
		}

		private Observable getReplyToObservable() {
			return replyToObservable;
		}
	}

	private class ReplyToConsumer<E extends Event<?>, V> implements Consumer<E> {
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
					replyEv = Event.NULL_EVENT;
				} else {
					replyEv = (Event.class.isAssignableFrom(reply.getClass()) ? (Event<?>) reply : Event.wrap(reply));
				}

				replyToObservable.notify(ev.getReplyTo(), replyEv);
			} catch (Throwable x) {
				replyToObservable.notify(x.getClass(), Event.wrap(x));
			}
		}
	}

	public static class Spec extends ComponentSpec<Reactor.Spec, Reactor> {
		private boolean link;

		public Spec() {
		}

		public Spec link() {
			this.link = true;
			return this;
		}

		@Override
		public Reactor configure(Reactor reactor) {
			if (link)
				this.reactor.link(reactor);

			return reactor;
		}
	}
}
