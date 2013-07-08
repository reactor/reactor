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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.convert.Converter;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.registry.CachingRegistry;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.registry.SelectionStrategy;
import reactor.event.routing.ArgumentConvertingConsumerInvoker;
import reactor.event.routing.ConsumerFilteringEventRouter;
import reactor.event.routing.EventRouter;
import reactor.event.routing.Linkable;
import reactor.event.selector.ClassSelector;
import reactor.event.selector.Selector;
import reactor.filter.PassThroughFilter;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Observable;
import reactor.function.Supplier;
import reactor.tuple.Tuple2;
import reactor.util.Assert;
import reactor.util.UUIDUtils;

import java.util.Set;
import java.util.UUID;

import static reactor.function.Functions.$;

/**
 * A reactor is an event gateway that allows other components to register {@link Event} (@link Consumer}s with its
 * {@link reactor.event.selector.Selector ) {@link reactor.event.registry.Registry }. When a {@literal Reactor} is
 * notified of that {@link Event}, a task is dispatched to the assigned {@link Dispatcher} which causes it to be
 * executed on a thread based on the implementation of the {@link Dispatcher} being used.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class Reactor implements Observable, Linkable<Observable> {

	private static final EventRouter DEFAULT_EVENT_ROUTER = new ConsumerFilteringEventRouter(
			new PassThroughFilter(), new ArgumentConvertingConsumerInvoker(null)
	);

	private final Environment                            env;
	private final Dispatcher                             dispatcher;
	private final Registry<Consumer<? extends Event<?>>> consumerRegistry;
	private final EventRouter                            eventRouter;

	private final Object   defaultKey      = new Object();
	private final Selector defaultSelector = $(defaultKey);

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
	 * reactor.event.registry.SelectionStrategy}, and {@link Converter} will be used.
	 *
	 * @param dispatcher The {@link Dispatcher} to use. May be {@code null} in which case a new worker dispatcher is used
	 *                   dispatcher is used
	 */
	public Reactor(Environment env,
								 Dispatcher dispatcher) {
		this(env,
				 dispatcher,
				 null,
				 null);
	}

	/**
	 * Create a new {@literal Reactor} that uses the given {@link Dispatcher}, {@link
	 * reactor.event.registry.SelectionStrategy}, {@link EventRouter}
	 *
	 * @param dispatcher        The {@link Dispatcher} to use. May be {@code null} in which case a new synchronous
	 *                          dispatcher is used.
	 * @param selectionStrategy The custom {@link SelectionStrategy} to use. May be {@code null}.
	 * @param eventRouter       The {@link EventRouter} used to route events to {@link Consumer Consumers}. May be {@code
	 *                          null} in which case a default event router is used.
	 */
	public Reactor(Environment env,
								 Dispatcher dispatcher,
								 SelectionStrategy selectionStrategy,
								 EventRouter eventRouter) {
		this.env = env;
		this.dispatcher = dispatcher == null ? new SynchronousDispatcher() : dispatcher;
		this.eventRouter = eventRouter == null ? DEFAULT_EVENT_ROUTER : eventRouter;
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
				if (null == log) {
					log = LoggerFactory.getLogger(Reactor.class);
				}
				log.error(ev.getData().getMessage(), ev.getData());
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

		dispatcher.dispatch(key, ev, consumerRegistry, errorHandler, eventRouter, onComplete);

		if (!linkedReactors.isEmpty()) {
			for (Observable r : linkedReactors) {
				r.notify(key, ev);
			}
		}
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
		return notify(key, Event.NULL_EVENT, null);
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

}
