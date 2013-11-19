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

import reactor.core.Observable;
import reactor.core.Reactor;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Predicate;
import reactor.operations.*;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract base class for components designed to provide a succinct API for working with future values. Provides base
 * functionality and an internal contract for subclasses that make use of the {@link #map(reactor.function.Function)}
 * and {@link #filter(reactor.function.Predicate)} methods.
 *
 * @param <T> The type of the values
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public abstract class Composable<T> {

	protected final ReentrantLock lock = new ReentrantLock();

	private final Tuple2<Selector, Object> accept = Selectors.$();
	private final Tuple2<Selector, Object> error  = Selectors.$();
	private final Tuple2<Selector, Object> flush  = Selectors.$();

	private final Observable    events;
	private final Composable<?> parent;

	private long acceptCount = 0l;
	private long errorCount  = 0l;

	protected <U> Composable(@Nonnull Dispatcher dispatcher, @Nullable Composable<U> parent) {
		Assert.notNull(dispatcher, "'dispatcher' cannot be null.");
		this.events = parent == null ? new Reactor(dispatcher) : parent.events;
		this.parent = parent;
		if (parent != null) {
			events.on(parent.error.getT1(),
					new ForwardOperation<Throwable>(events, error.getT2(), null));
		}
	}



	/**
	 * Assign an error handler to exceptions of the given type.
	 *
	 * @param exceptionType the type of exceptions to handle
	 * @param onError       the error handler for each exception
	 * @param <E>           type of the exception to handle
	 * @return {@literal this}
	 */
	public <E extends Throwable> Composable<T> when(@Nonnull final Class<E> exceptionType,
	                                                @Nonnull final Consumer<E> onError) {
		this.events.on(error.getT1(), new Consumer<Event<E>>(){
			@Override
			public void accept(Event<E> e) {
				if(Selectors.T(exceptionType).matches(e.getData().getClass())){
					onError.accept(e.getData());
				}
			}
		});
		return this;
	}
	/**
	 * Attach another {@code Composable} to this one that will cascade the value received by this {@code Composable} into
	 * the next.
	 *
	 * @param composable the next {@code Composable} to cascade events to
	 * @return {@literal this}
	 */
	public Composable<T> consume(@Nonnull final Composable<T> composable) {
		if (composable == this) {
			throw new IllegalArgumentException("Trying to consume itself, leading to erroneous recursive calls");
		}
		addOperation(new ForwardOperation<T>(composable.events, composable.accept.getT2(), error.getT2()));
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
		addOperation(new CallbackOperation<T>(consumer, events, error.getT2()));
		return this;
	}

	/**
	 * Attach a {@link Consumer<Event>} to this {@code Composable} that will consume any values accepted by this {@code
	 * Composable}.
	 *
	 * @param consumer the conumer to invoke on each value
	 * @return {@literal this}
	 */
	public Composable<T> consumeEvent(@Nonnull final Consumer<Event<T>> consumer) {
		addOperation(new CallbackEventOperation<T>(consumer, events, error.getT2()));
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
		this.events.on(accept.getT1(), new ForwardOperation<T>(observable, key, null));
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
		addOperation(new MapOperation<T, V>(fn, events, d.compose().getAccept().getT2(), error.getT2()));
		return d.compose();
	}

	/**
	 * Evaluate each accepted value against the given predicate {@link Function}. If the predicate test succeeds, the
	 * value is passed into the new {@code Composable}. If the predicate test fails, an exception is propagated into the
	 * new {@code Composable}.
	 *
	 * @param fn the predicate {@link Function} to test values against
	 * @return a new {@code Composable} containing only values that pass the predicate test
	 */
	public Composable<T> filter(@Nonnull final Function<T, Boolean> fn) {
		return filter(new Predicate<T>() {
			@Override
			public boolean test(T t) {
				return fn.apply(t);
			}
		}, null);
	}

	/**
	 * Evaluate each accepted value against the given {@link Predicate}. If the predicate test succeeds, the value is
	 * passed into the new {@code Composable}. If the predicate test fails, the value is ignored.
	 *
	 * @param p the {@link Predicate} to test values against
	 * @return a new {@code Composable} containing only values that pass the predicate test
	 */
	public Composable<T> filter(@Nonnull final Predicate<T> p) {
		return filter(p, null);
	}

	/**
	 * Evaluate each accepted value against the given {@link Predicate}. If the predicate test succeeds, the value is
	 * passed into the new {@code Composable}. If the predicate test fails, the value is propagated into the {@code
	 * elseComposable}.
	 *
	 * @param p              the {@link Predicate} to test values against
	 * @param elseComposable the optional {@link reactor.core.composable.Composable} to pass rejected values
	 * @return a new {@code Composable} containing only values that pass the predicate test
	 */
	public Composable<T> filter(@Nonnull final Predicate<T> p, final Composable<T> elseComposable) {
		final Deferred<T, ? extends Composable<T>> d = createDeferred();
		addOperation(new FilterOperation<T>(p, events, d.compose().getAccept().getT2(), error.getT2(),
				elseComposable.events,
				elseComposable.accept.getT2()));
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
	 * Flush any cached or unprocessed values through this {@literal Stream}.
	 *
	 * @return {@literal this}
	 */
	public Composable<T> flush() {
		if (null != parent) {
			parent.flush();
		}
		events.notify(flush.getT2(), new Event<Void>(null));
		return this;
	}

	/**
	 * Notify this {@code Composable} that a value is being accepted by this {@code Composable}.
	 *
	 * @param value the value to accept
	 */
	void notifyValue(T value) {
		notifyValue(Event.wrap(value));
	}

	void notifyValue(Event<T> value) {
		lock.lock();
		try {
			valueAccepted(value.getData());
			acceptCount++;
		} finally {
			lock.unlock();
		}
		events.notify(accept.getT2(), value);
	}

	/**
	 * Notify this {@code Composable} that an error is being propagated through this {@code Composable}.
	 *
	 * @param error the error to propagate
	 */
	void notifyError(Throwable error) {
		lock.lock();
		try {
			errorAccepted(error);
			errorCount++;
		} finally {
			lock.unlock();
		}
		events.notify(this.error.getT2(), Event.wrap(error));
	}

	/**
	 * Consume events with the passed {@code Operation}
	 *
	 * @param operation the operation listening for values
	 */
	protected Composable<T> addOperation(Operation<Event<T>> operation){
		this.events.on(accept.getT1(), operation);
		return this;
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
	 * Called before {@code errorCount} has been incremented and {@link Consumer}s have been notified.
	 *
	 * @param error the error being propagated
	 */
	protected abstract void errorAccepted(Throwable error);

	/**
	 * Called before {@code acceptCount} has been incremented and {@link Consumer}s have been notified.
	 *
	 * @param value the value being accepted
	 */
	protected abstract void valueAccepted(T value);

	/**
	 * Get the current {@link Observable}.
	 *
	 * @return
	 */
	protected Observable getObservable() {
		return events;
	}

	/**
	 * Get the anonymous {@link Selector} and notification key {@link Tuple2} for doing accepts.
	 *
	 * @return
	 */
	protected Tuple2<Selector, Object> getAccept() {
		return this.accept;
	}

	/**
	 * Get the parent {@link Composable} for callback callback.
	 *
	 * @return
	 */
	protected Composable<?> getParent() {
		return this.parent;
	}

	/**
	 * Get the anonymous {@link Selector} and notification key {@link Tuple2} for doing flushes.
	 *
	 * @return
	 */
	protected Tuple2<Selector, Object> getFlush() {
		return this.flush;
	}
}
