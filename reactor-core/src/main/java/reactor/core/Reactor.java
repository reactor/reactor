/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package reactor.core;

import com.eaio.uuid.UUID;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import reactor.Fn;
import reactor.convert.Converter;
import reactor.fn.*;
import reactor.fn.dispatch.Dispatcher;
import reactor.fn.dispatch.DispatcherAware;
import reactor.fn.dispatch.Task;
import reactor.support.Assert;

import java.util.Set;

import static reactor.Fn.$;
import static reactor.Fn.T;

/**
 * A reactor is an event gateway that allows other components to register {@link Event} (@link Consumer}s with its
 * {@link Selector) {@link Registry}. When a {@literal Reactor} is notified of that {@link Event}, a task is dispatched
 * to the assigned {@link Dispatcher} which causes it to be executed on a thread based on the implementation of the
 * {@link Dispatcher} being used.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class Reactor implements Observable, DispatcherAware, Linkable<Reactor> {

	private static final Event<Void> NULL_EVENT = new Event<Void>(null);

	protected final Object              defaultKey = new Object();
	protected final Selector            defaultSelector = $(defaultKey);
	protected final UUID                id               = new UUID();
	protected final Consumer<Throwable> errorHandler     = new Consumer<Throwable>() {
		@Override
		public void accept(Throwable t) {
			Reactor.this.notify(T(t.getClass()), Fn.event(t));
		}
	};
	protected final Set<Reactor>        linkedReactors   = new NonBlockingHashSet<Reactor>();

	protected volatile Registry<Consumer<? extends Event<?>>> consumerRegistry = new CachingRegistry<Consumer<? extends Event<?>>>();
	protected volatile SelectionStrategy selectionStrategy;
	protected volatile Dispatcher        dispatcher;
	protected volatile Converter         converter;

	/**
	 * Copy constructor that creates a shallow copy of the given {@link Reactor} minus the {@link Registry}. Each {@literal
	 * Reactor} needs to maintain its own {@link Registry} to keep the {@link Consumer}s registered on the given {@literal
	 * Reactor} from being triggered on the new {@literal Reactor}.
	 *
	 * @param src The {@literal Reactor} from which to get the {@link SelectionStrategy}, {@link Converter}, and {@link
	 *            Dispatcher}.
	 */
	public Reactor(Reactor src) {
		this(src.getDispatcher());
		this.selectionStrategy = src.getSelectionStrategy();
		this.converter = src.getConverter();
	}

	/**
	 * Create a new {@literal Reactor} that uses the given {@link Dispatcher}.
	 *
	 * @param dispatcher The {@link Dispatcher} to use.
	 */
	public Reactor(Dispatcher dispatcher) {
		if (dispatcher == null) {
			this.dispatcher = Context.nextWorkerDispatcher();
		} else {
			this.dispatcher = dispatcher;
		}
	}

	/**
	 * Create a new {@literal Reactor} with a newly-assigned worker {@link Dispatcher}.
	 *
	 * @see {@link Context#nextWorkerDispatcher()}
	 */
	public Reactor() {
		this(Context.nextWorkerDispatcher());
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

	public Reactor setConsumerRegistry(Registry<Consumer<? extends Event<?>>> consumerRegistry) {
		Assert.notNull(consumerRegistry, "Registry cannot be null.");
		this.consumerRegistry = consumerRegistry;
		return this;
	}

	/**
	 * Get the {@link SelectionStrategy} to use to help the {@link Dispatcher} find which {@link Selector}s in the {@link
	 * Registry} match the key triggered by the given event.
	 *
	 * @return The {@link SelectionStrategy} in use. Defaults to {@literal null}.
	 */
	public SelectionStrategy getSelectionStrategy() {
		return selectionStrategy;
	}

	/**
	 * Set the {@link SelectionStrategy} to use to help the {@link Dispatcher} find which {@link Selector}s in the {@link
	 * Registry} match the key triggered by the given event.
	 *
	 * @param selectionStrategy The {@link SelectionStrategy} in use. Can be {@literal null} to indicate that the {@link
	 *                          Dispatcher} should use the default selection behaviour.
	 * @return {@literal this}
	 */
	public Reactor setSelectionStrategy(SelectionStrategy selectionStrategy) {
		this.selectionStrategy = selectionStrategy;
		return this;
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
	 * Set the {@link Dispatcher} to use when executing {@link Task}s.
	 *
	 * @param dispatcher The {@link Dispatcher} to use.
	 * @return {@literal this}
	 */
	public Reactor setDispatcher(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		return this;
	}

	/**
	 * Get the {@link Converter} in use for converting arguments to {@link Consumer}s.
	 *
	 * @return The {@link Converter} in use.
	 */
	public Converter getConverter() {
		return converter;
	}

	/**
	 * Set the {@link Converter} to use for converting arguments to {@link Consumer}s.
	 *
	 * @param converter The {@link Converter} to use.
	 * @return {@literal this}
	 */
	public Reactor setConverter(Converter converter) {
		this.converter = converter;
		return this;
	}

	@Override
	public boolean respondsToKey(Object key) {
		Assert.notNull(key, "Key cannot be null.");
		return consumerRegistry.select(key).iterator().hasNext();
	}

	@Override
	public <T, E extends Event<T>> Registration<Consumer<E>> on(Selector selector, final Consumer<E> consumer) {
		Assert.notNull(selector, "Selector cannot be null.");
		Assert.notNull(consumer, "Consumer cannot be null.");
		return consumerRegistry.register(selector, consumer);
	}

	@Override
	public <T, E extends Event<T>> Registration<Consumer<E>> on(Consumer<E> consumer) {
		Assert.notNull(consumer, "Consumer cannot be null.");
		return consumerRegistry.register(defaultSelector, consumer);
	}

	@Override
	public <T, E extends Event<T>, V> Registration<Consumer<E>> receive(Selector sel, Function<E, V> fn) {
		return on(sel, new ReplyToConsumer<T, E, V>(fn));
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T, E extends Event<T>> Reactor notify(Object key, E ev, Consumer<E> onComplete) {
		Assert.notNull(key, "Key cannot be null.");
		Assert.notNull(ev, "Event cannot be null.");

		Task<T> task = dispatcher.nextTask();
		task.setKey(key);
		task.setEvent(ev);
		task.setConverter(converter);
		task.setConsumerRegistry(consumerRegistry);
		task.setErrorConsumer(errorHandler);
		task.setCompletionConsumer((Consumer<Event<T>>) onComplete);
		task.submit();

		if (!linkedReactors.isEmpty()) {
			for (Reactor r : linkedReactors) {
				r.notify(key, ev);
			}
		}

		return this;
	}

	@Override
	public <T, E extends Event<T>> Reactor notify(Object key, E ev) {
		return notify(key, ev, null);
	}

	@Override
	public <T, S extends Supplier<Event<T>>> Reactor notify(Object key, S supplier) {
		return notify(key, supplier.get(), null);
	}

	@Override
	public <T, E extends Event<T>> Reactor notify(E ev) {
		return notify(defaultKey, ev, null);
	}

	@Override
	public <T, S extends Supplier<Event<T>>> Reactor notify(S supplier) {
		return notify(defaultKey, supplier.get(), null);
	}

	@Override
	public Reactor notify(Object key) {
		return notify(key, NULL_EVENT, null);
	}

	@Override
	public <T, E extends Event<T>> Reactor send(Object key, E ev) {
		return notify(key, new ReplyToEvent<T>(ev, this));
	}

	@Override
	public <T, S extends Supplier<Event<T>>> Reactor send(Object key, S supplier) {
		return notify(key, new ReplyToEvent<T>(supplier.get(), this));
	}

	@Override
	public <T, E extends Event<T>> Reactor send(Object key, E ev, Observable replyTo) {
		return notify(key, new ReplyToEvent<T>(ev, replyTo));
	}

	@Override
	public <T, S extends Supplier<Event<T>>> Reactor send(Object key, S supplier, Observable replyTo) {
		return notify(key, new ReplyToEvent<T>(supplier.get(), replyTo));
	}

	/**
	 * Register a {@link Function} to be triggered using the given {@link Selector}. The return value is automatically
	 * handled and replied to the origin reactor/replyTo key set within an incoming event
	 *
	 * @param sel      The {@link Selector} to be used for matching
	 * @param function The {@literal Function} to be triggered.
	 * @param <T>      The type of the data in the {@link Event}.
	 * @param <V>      The type of the return value from the given {@link Function}.
	 * @return A {@link Registration} object that allows the caller to interact with the given mapping.
	 */
	public <T, E extends Event<T>, V> Composable<V> map(Selector sel, final Function<E, V> function) {
		Assert.notNull(sel, "Selector cannot be null.");
		Assert.notNull(function, "Function cannot be null.");

		final Composable<V> c = new Composable<V>(new Reactor(this));
		on(sel, new Consumer<E>() {
			@Override
			public void accept(E event) {
				try {
					c.accept(function.apply(event));
				} catch (Throwable t) {
					c.accept(t);
				}
			}
		});
		return c;
	}

	/**
	 * Register a {@link Function} to be triggered using the internal, global {@link Selector} that is unique to each
	 * {@literal Observable} instance. The return value is automatically handled and replied to the origin reactor/replyTo
	 * key set within an incoming event
	 *
	 * @param function The {@literal Consumer} to be triggered.
	 * @param <T>      The type of the data in the {@link Event}.
	 * @param <V>      The type of the return value from the given {@link Function}.
	 * @return A {@link Registration} object that allows the caller to interact with the given mapping.
	 */
	public <T, E extends Event<T>, V> Composable<V> map(Function<E, V> function) {
		Assert.notNull(function, "Function cannot be null.");
		return map(defaultSelector, function);
	}

	/**
	 * Notify this component that the consumers that match {@param key} should consume the event {@param ev} and might
	 * reply using R.replyTo (which is automatically handled when used in combination with {@link #map(Function)}.
	 *
	 * @param key The notification key
	 * @param ev  The {@link Event} to publish.
	 * @return {@link Composable}
	 */
	public <T, V> Composable<V> compose(Object key, T ev) {
		Composable<T> composable = new Composable<T>(new Reactor(this));
		Composable<V> reply = composable.map(key, this);

		composable.accept(ev);

		return reply;
	}

	/**
	 * Compose an action starting with the given key and {@link Supplier}.
	 *
	 * @param key      The key to use.
	 * @param supplier The {@link Supplier} that will provide the actual {@link Event} instance.
	 * @param <T>      The type of the incoming data.
	 * @param <S>      The type of the event supplier.
	 * @param <V>      The type of the {@link Composable}.
	 * @return A new {@link Composable}.
	 */
	public <T, S extends Supplier<Event<T>>, V> Composable<V> compose(Object key, S supplier) {
		return compose(key, supplier.get());
	}

	@Override
	public Reactor link(Reactor reactor) {
		linkedReactors.add(reactor);
		return this;
	}

	@Override
	public Reactor unlink(Reactor reactor) {
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

	private class ReplyToConsumer<T, E extends Event<T>, V> implements Consumer<E> {
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
					replyEv = NULL_EVENT;
				} else {
					replyEv = (Event.class.isAssignableFrom(reply.getClass()) ? (Event<?>) reply : Fn.event(reply));
				}

				replyToObservable.notify(ev.getReplyTo(), replyEv);
			} catch (Throwable x) {
				replyToObservable.notify(T(x.getClass()), Fn.event(x));
			}
		}
	}

}
