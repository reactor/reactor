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

import org.reactivestreams.spi.Subscriber;
import reactor.core.Environment;
import reactor.core.Observable;
import reactor.core.composable.action.*;
import reactor.event.dispatch.Dispatcher;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Predicate;
import reactor.function.Supplier;
import reactor.timer.Timer;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Abstract base class for components designed to provide a succinct API for working with future values. Provides base
 * functionality and an internal contract for subclasses that make use of the {@link #map(reactor.function.Function)}
 * and {@link #filter(reactor.function.Predicate)} methods.
 *
 * @param <T> The type of the values
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
public abstract class Composable<T> implements Pipeline<T> {

	private final Environment        environment;
	private final Composable<?>      parent;
	private final ActionProcessor<T> actionProcessor;
	private final Dispatcher         dispatcher;
	private final int                batchSize;

	protected <U> Composable(@Nullable Dispatcher dispatcher, @Nullable Composable<U> parent) {
		this(dispatcher, parent, null, parent != null ? parent.batchSize : 1024);
	}


	protected <U> Composable(@Nullable Dispatcher dispatcher,
	                         @Nullable Composable<U> parent,
	                         @Nullable Environment environment,
	                         int batchSize) {
		Assert.state(dispatcher != null || parent != null, "One of 'dispatcher' or 'parent'  cannot be null.");
		this.parent = parent;
		this.environment = environment;
		this.batchSize = batchSize < 0 ? 1 : batchSize;
		this.dispatcher = parent == null ? dispatcher : parent.dispatcher;
		this.actionProcessor = new ActionProcessor<T>(
				-1l
		);

	}


	/**
	 * Assign an error handler to exceptions of the given type.
	 *
	 * @param exceptionType the type of exceptions to handle
	 * @param onError       the error handler for each exception
	 * @param <E>           type of the exception to handle
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public <E extends Throwable> Composable<T> when(@Nonnull final Class<E> exceptionType,
	                                                @Nonnull final Consumer<E> onError) {
		action(new ErrorAction<T, E>(dispatcher, Selectors.T(exceptionType), onError));
		return this;
	}


	/**
	 * Materialize an error state into a downstream event.
	 *
	 * @param exceptionType the type of exceptions to handle
	 * @param <E>           type of the exception to handle
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public <E extends Throwable> Composable<E> recover(@Nonnull final Class<E> exceptionType) {
		Composable<E> output = newComposable();
		action(new RecoverAction<T, E>(dispatcher, output.actionProcessor, Selectors.T(exceptionType)));
		return output;
	}

	/**
	 * @return {@literal this}
	 * @see {@link org.reactivestreams.api.Producer#produceTo(org.reactivestreams.api.Consumer)}
	 * @since 1.1
	 */
	public Composable<T> connect(@Nonnull final Composable<T> composable) {
		this.produceTo(composable);
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
		action(new CallbackAction<T>(dispatcher, consumer));
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
		action(new ObservableAction<T>(dispatcher, observable, key));
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
		final Composable<V> d = newComposable();
		action(new MapAction<T, V>(
				fn,
				dispatcher,
				d.actionProcessor)
		);

		return d;
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code Composable<V>} and pass
	 * it into another {@code Composable}.
	 *
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@code Composable} containing the transformed values
	 * @since 1.1
	 */
	public <V, C extends Composable<V>> Composable<V> mapMany(@Nonnull final Function<T, C> fn) {
		Assert.notNull(fn, "FlatMap function cannot be null.");
		final Composable<V> d = newComposable();
		action(new MapManyAction<T, V, C>(
				fn,
				dispatcher,
				d.actionProcessor
		));
		return d;
	}

	/**
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@code Composable} containing the transformed values
	 * @see {@link org.reactivestreams.api.Producer#produceTo(org.reactivestreams.api.Consumer)}
	 * @since 1.1
	 */
	public <V, C extends Composable<V>> Composable<V> flatMap(@Nonnull final Function<T, C> fn) {
		return mapMany(fn);
	}

	/**
	 * {@link this#connect(Composable)} all the passed {@param composables} to this {@link Composable},
	 * merging values streams into the current pipeline.
	 *
	 * @param composables the the composables to connect
	 * @return this composable
	 * @since 1.1
	 */
	public Composable<T> merge(Composable<T>... composables) {
		for (Composable<T> composable : composables) {
			composable.connect(this);
		}
		return this;
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
	 * Evaluate each accepted boolean value. If the predicate test succeeds, the value is
	 * passed into the new {@code Composable}. If the predicate test fails, the value is ignored.
	 *
	 * @return a new {@code Composable} containing only values that pass the predicate test
	 * @since 1.1
	 */
	@SuppressWarnings("unchecked")
	public Composable<Boolean> filter() {
		return ((Composable<Boolean>) this).filter(FilterAction.simplePredicate, null);
	}

	/**
	 * Evaluate each accepted boolean value. If the predicate test succeeds,
	 * the value is passed into the new {@code Composable}. the value is propagated into the {@param
	 * elseComposable}.
	 *
	 * @param elseComposable the {@link Composable} to test values against
	 * @return a new {@code Composable} containing only values that pass the predicate test
	 * @since 1.1
	 */
	@SuppressWarnings("unchecked")
	public Composable<Boolean> filter(@Nonnull final Composable<Boolean> elseComposable) {
		return ((Composable<Boolean>) this).filter(FilterAction.simplePredicate, elseComposable);
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
		final Composable<T> d = newComposable();
		action(new FilterAction<T>(p, dispatcher, d.actionProcessor,
				elseComposable != null ? elseComposable.actionProcessor : null));
		return d;
	}


	/**
	 * Flush the parent if any or the current composable otherwise when the last notification occurred before {@param
	 * timeout} milliseconds. Timeout is run on the environment root timer.
	 *
	 * @param timeout the timeout in milliseconds between two notifications on this composable
	 * @return this {@link Composable}
	 * @since 1.1
	 */
	public Composable<T> timeout(long timeout) {
		Assert.state(environment != null, "Cannot use default timer as no environment has been provided to this Stream");
		return timeout(timeout, environment.getRootTimer());
	}

	/**
	 * Flush the parent if any or the current composable otherwise when the last notification occurred before {@param
	 * timeout} milliseconds. Timeout is run on the provided {@param timer}.
	 *
	 * @param timeout the timeout in milliseconds between two notifications on this composable
	 * @param timer   the reactor timer to run the timeout on
	 * @return this {@link Composable}
	 * @since 1.1
	 */
	public Composable<T> timeout(long timeout, Timer timer) {
		Assert.state(timer != null, "Timer must be supplied");
		Composable<?> composable = parent != null ? parent : this;

		action(new TimeoutAction<T>(
				dispatcher,
				composable.actionProcessor,
				timer,
				timeout
		));

		return this;
	}

	/**
	 * Create a new {@code Composable} whose values will be generated from {@param supplier}.
	 * Every time flush is triggered, {@param supplier} is called.
	 *
	 * @param supplier the supplier to drain
	 * @return a new {@code Composable} whose values are generated on each flush
	 * @since 1.1
	 */
	public Composable<T> propagate(final Supplier<T> supplier) {

		final Composable<T> d = newComposable();
		produceTo(new SupplierAction<T,T>(dispatcher, d.actionProcessor, supplier));
		return d;
	}

	/**
	 * Flush any cached or unprocessed values through this {@literal Stream}.
	 *
	 * @return {@literal this}
	 */
	public Composable<T> flush() {
		Composable<?> that = this;
		while (that.parent != null) {
			that = that.parent;
		}

		that.actionProcessor.flush();
		return this;
	}

	/**
	 * Complete this {@literal Stream}.
	 *
	 * @return {@literal this}
	 */
	public Composable<T> complete() {
		Composable<?> that = this;
		while (that.parent != null) {
			that = that.parent;
		}

		that.actionProcessor.onComplete();
		return this;
	}

	/**
	 * Print a debugged form of the root composable relative to this. The output will be an acyclic directed graph of
	 * composed actions.
	 *
	 * @since 1.1
	 */
	public String debug() {
		Composable<?> that = this;
		while (that.parent != null) {
			that = that.parent;
		}
		return ActionUtils.browseComposable(that);
	}

	@Override
	public Subscriber<T> getSubscriber() {
		return actionProcessor;
	}

	@Override
	public ActionProcessor<T> getPublisher() {
		return actionProcessor;
	}

	@Override
	public void produceTo(org.reactivestreams.api.Consumer<T> consumer) {
		actionProcessor.subscribe(consumer.getSubscriber());
	}

	/**
	 * Attach the given action to consume the stream of data from {@link this} composable
	 *
	 * @param consumer The action to consume that composable's values
	 */
	public void action(Action<T,?> consumer) {
		actionProcessor.subscribe(consumer.prefetch(batchSize));
	}

	/**
	 * Return defined {@link Composable} batchSize, used to drive new {@link org.reactivestreams.spi.Subscription}
	 * request needs.
	 * @return
	 */
	public int getBatchSize() {
		return batchSize;
	}

	/**
	 * Create a {@link Composable} that is compatible with the subclass of {@code Composable} in use.
	 *
	 * @param <V> type the {@code Composable} handles
	 * @return a new {@code Composable} compatible with the current subclass.
	 */
	protected abstract <V> Composable<V> newComposable();

	/**
	 * Get the parent {@link Composable} for callback callback.
	 *
	 * @return
	 */
	protected Composable<?> getParent() {
		return this.parent;
	}

	/**
	 * Get the assigned {@link reactor.core.Environment}.
	 *
	 * @return
	 */
	protected Environment getEnvironment() {
		return environment;
	}

	/**
	 * Get the assigned {@link reactor.event.dispatch.Dispatcher}.
	 *
	 * @return
	 */
	protected Dispatcher getDispatcher() {
		return dispatcher;
	}
}
