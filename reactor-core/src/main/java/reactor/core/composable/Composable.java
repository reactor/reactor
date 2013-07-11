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

package reactor.core.composable;

import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import reactor.core.Reactor;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.event.support.EventConsumer;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Observable;
import reactor.function.Predicate;
import reactor.function.support.NotifyConsumer;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

/**
 * Abstract base class for components designed to provide a succinct API for working with future values. Provides base
 * functionality and an internal contract for subclasses that make use of the {@link #map(reactor.function.Function)} and
 * {@link #filter(reactor.function.Predicate)} methods.
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @author Andy Wilkinson
 *
 * @param <T> The type of the values
 */
public abstract class Composable<T> {

	protected final ReentrantLock lock = new ReentrantLock();

	private final Tuple2<Selector, Object> accept = Selectors.$();

	private final Observable    events;
	private final Composable<?> parent;

	private volatile long acceptCount = 0l;
	private volatile long errorCount  = 0l;

	protected <U> Composable(@Nonnull Dispatcher dispatcher,
													 @Nullable Composable<U> parent) {
		Assert.notNull(dispatcher, "'dispatcher' cannot be null.");
		this.events = new Reactor(dispatcher);
		this.parent = parent;
		if (parent != null) {
			parent.cascadeErrors(this);
		}
	}

	/**
	 * Attach another {@code Composable} to this one that will cascade the value received by this {@code Composable} into
	 * the next.
	 *
	 * @param composable the next {@code Composable} to cascade events to
	 * @return {@literal this}
	 */
	public Composable<T> consume(@Nonnull final Composable<T> composable) {
		this.events.on(accept.getT1(), new EventConsumer<T>(new Consumer<T>() {
			@Override
			public void accept(T event) {
				composable.notifyValue(event);
			}
		}));
		cascadeErrors(composable);
		return this;
	}

	/**
	 * Attach a {@link Consumer} to this {@code Composable} that will consume any values accepted by this {@code
	 * Composable}.
	 *
	 * @param consumer the conumer to invoke on each value
	 * @return {@literal this}
	 */
	public Composable<T> consume(@Nonnull final Consumer<T> consumer) {
		this.events.on(accept.getT1(), new EventConsumer<T>(consumer));
		return this;
	}

	/**
	 * Pass values accepted by this {@code Composable} into the given {@link Observable}, notifying with the given key.
	 *
	 * @param key        the key to notify on
	 * @param observable the {@link Observable} to notify
	 * @return {@literal this}
	 */
	public Composable<T> consume(@Nonnull final Object key, @Nonnull final Observable observable) {
		consume(new NotifyConsumer<T>(key, observable));
		return this;
	}

	/**
	 * Assign an error handler to exceptions of the given type.
	 *
	 * @param exceptionType the type of exceptions to handle
	 * @param onError       the error handler for each exception
	 * @param <E>           type of the exception to handle
	 * @return {@literal this}
	 */
	public <E extends Throwable> Composable<T> when(@Nonnull Class<E> exceptionType, @Nonnull Consumer<E> onError) {
		this.events.on(Selectors.T(exceptionType), new EventConsumer<E>(onError));
		return this;
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code V} and pass it into
	 * another {@code Composable}.
	 *
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@code Composable} containing the transformed values
	 */
	public <V> Composable<V> map(@Nonnull final Function<T, V> fn) {
		Assert.notNull(fn, "Map function cannot be null.");
		final Deferred<V, ? extends Composable<V>> d = createDeferred();
		consume(new Consumer<T>() {
			@Override
			public void accept(T value) {
				try {
					V val = fn.apply(value);
					d.accept(val);
				} catch (Throwable e) {
					d.accept(e);
				}
			}
		});
		return d.compose();
	}

	/**
	 * Evaluate each accepted value against the given {@link Predicate}. If the predicate test succeeds, the value is
	 * passed into the new {@code Composable}. If the predicate test fails, an exception is propagated into the new {@code
	 * Composable}.
	 *
	 * @param p the {@link Predicate} to test values against
	 * @return a new {@code Composable} containing only values that pass the predicate test
	 */
	public Composable<T> filter(@Nonnull final Predicate<T> p) {
		final Deferred<T, ? extends Composable<T>> d = createDeferred();
		consume(new Consumer<T>() {
			@Override
			public void accept(T value) {
				boolean b = p.test(value);
				if (b) {
					d.accept(value);
				} else {
					d.accept(new IllegalArgumentException(String.format("%s failed a predicate test.", value)));
				}
			}
		});
		return d.compose();
	}

	/**
	 * Get the total number of values accepted into this {@code Composable} since its creation.
	 *
	 * @return number of values accepted
	 */
	public long getAcceptCount() {
		lock.lock();
		try {
			return acceptCount;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Get the total number of errors propagated through this {@code Composable} since its creation.
	 *
	 * @return number of errors propagated
	 */
	public long getErrorCount() {
		lock.lock();
		try {
			return errorCount;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * {@code Composable}s created with existing values must be accepted into this {@code Composable} after all event
	 * handlers have been assigned. {@link #resolve()} is called to trigger this acceptance of initial values.
	 *
	 * @return {@literal this}
	 */
	public Composable<T> resolve() {
		if (null != parent) {
			parent.resolve();
		}
		doResolution();
		return this;
	}

	/**
	 * Notify this {@code Composable} that a value is being accepted by this {@code Composable}.
	 *
	 * @param value the value to accept
	 */
	void notifyValue(T value) {
		lock.lock();
		try {
			acceptCount++;
			valueAccepted(value);
		} finally {
			lock.unlock();
		}
		events.notify(accept.getT2(), Event.wrap(value));
	}

	/**
	 * Notify this {@code Composable} that an error is being propagated through this {@code Composable}.
	 *
	 * @param error the error to propagate
	 */
	void notifyError(Throwable error) {
		lock.lock();
		try {
			errorCount++;
			errorAccepted(error);
		} finally {
			lock.unlock();
		}
		events.notify(error.getClass(), Event.wrap(error));
	}

	/**
	 * Create a {@link Deferred} that is compatible with the subclass of {@code Composable} in use.
	 *
	 * @param <V> type the {@code Composable} handles
	 * @param <C> type of the {@code Composable} returned by the {@link Deferred#compose()} method.
	 * @return a new {@link Deferred} backed by a {@code Composable} compatible with the current subclass.
	 */
	protected abstract <V, C extends Composable<V>> Deferred<V, C> createDeferred();

	/**
	 * Called after {@code errorCount} has been incremented, but before {@link Consumer}s have been notified.
	 *
	 * @param error the error being propagated
	 */
	protected abstract void errorAccepted(Throwable error);

	/**
	 * Called after {@code acceptCount} has been incremented, but before {@link Consumer}s have been notified.
	 *
	 * @param value the value being accepted
	 */
	protected abstract void valueAccepted(T value);

	/**
	 * Subclasses must implement this to do resolution processing.
	 */
	protected abstract void doResolution();

	/**
	 * Adds an error {@link Consumer} to this {@code Composable} that will propagate errors to the given {@link
	 * Composable}.
	 *
	 * @param composable the {@code Composable} to propagate errors to
	 */
	protected void cascadeErrors(final Composable<?> composable) {
		when(Throwable.class, new Consumer<Throwable>() {
			@Override
			public void accept(Throwable t) {
				composable.notifyError(t);
			}
		});
	}

	/**
	 * Get the current {@link Observable}.
	 *
	 * @return
	 */
	protected Observable getObservable() {
		return events;
	}

}
