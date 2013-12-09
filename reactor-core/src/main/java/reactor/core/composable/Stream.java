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

import reactor.core.action.*;
import reactor.core.Environment;
import reactor.core.Observable;
import reactor.core.composable.spec.DeferredStreamSpec;
import reactor.event.dispatch.Dispatcher;
import reactor.event.selector.Selector;
import reactor.function.*;
import reactor.function.support.Tap;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * A {@code Stream} is a stateless event processor that provides methods for attaching {@link
 * Consumer Consumers} to consume the values passing through the stream. Some methods like
 * {@link #map(reactor.function.Function)}, {@link #filter(reactor.function.Predicate)},
 * {@link #first()}, {@link #last()}, and {@link #reduce(reactor.function.Function, Object)}
 * return new {@code Stream Streams} whose values are the result of transformation functions,
 * filtering, and the like.
 * <p/>
 * Typically, new {@code Stream Streams} aren't created directly. To create a {@code Stream},
 * create a {@link DeferredStreamSpec} and configure it with the appropriate {@link Environment},
 * {@link Dispatcher}, and other settings, then call {@link Deferred#compose()}, which will
 * return a {@code Stream} that handles the values passed into the {@link Deferred}.
 *
 * @param <T> the type of the values in the stream
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public class Stream<T> extends Composable<T> {

	private final int         batchSize;

	/**
	 * Create a new Stream that will use the {@link Observable} to pass its values to registered
	 * handlers.
	 * <p>
	 * The stream will batch values into batches of the given
	 * {@code batchSize}, affecting the values that are passed to the {@link #first()} and {@link
	 * #last()} substreams. A size of {@code -1} indicates that the stream should not be batched.
	 * </p>
	 * The stream will be pre-populated with the given initial {@code values}. Once all event
	 * handler have been registered, {@link #flush} must be called to pass the initial values
	 * to those handlers.
	 * The stream will accept errors from the given {@code parent}.
	 *
	 * @param observable The observable used to drive event handlers
	 * @param batchSize  The size of the batches, or {@code -1} for no batching
	 * @param values     The stream's initial values. May be {@code null}
	 * @param parent     The stream's parent. May be {@code null}
	 */
	public Stream(@Nullable final Observable observable,
	              int batchSize,
	              @Nullable Iterable<T> values,
	              @Nullable final Composable<?> parent) {
		this(observable, batchSize, values, parent, null);
	}
	/**
	 * Create a new Stream that will use the {@link Observable} to pass its values to registered
	 * handlers.
	 * <p>
	 * The stream will batch values into batches of the given
	 * {@code batchSize}, affecting the values that are passed to the {@link #first()} and {@link
	 * #last()} substreams. A size of {@code -1} indicates that the stream should not be batched.
	 * </p>
	 * The stream will be pre-populated with the given initial {@code values}. Once all event
	 * handler have been registered, {@link #flush} must be called to pass the initial values
	 * to those handlers.
	 * The stream will accept errors from the given {@code parent}.
	 *
	 * @param observable The observable used to drive event handlers
	 * @param batchSize  The size of the batches, or {@code -1} for no batching
	 * @param values     The stream's initial values. May be {@code null}
	 * @param parent     The stream's parent. May be {@code null}
	 * @param acceptSelector     The tuple Selector/Key to accept values on this observable. May be {@code null}
	 */
	public Stream(@Nullable final Observable observable,
	              int batchSize,
	              @Nullable Iterable<T> values,
	              @Nullable final Composable<?> parent, Tuple2<Selector, Object> acceptSelector) {
		super(observable, parent, acceptSelector);
		this.batchSize = batchSize;

		attachFlush(values);
	}

	private void attachFlush(Iterable<T> values) {
		if (values != null) {
			new ForEachAction<T>(values, getObservable(), getAccept().getT2(), getError().getT2()).
					attach(getFlush().getT1());
		}
	}

	@Override
	public Stream<T> consume(@Nonnull Consumer<T> consumer) {
		return (Stream<T>) super.consume(consumer);
	}

	@Override
	public Stream<T> connect(@Nonnull Composable<T> consumer) {
		return (Stream<T>) super.connect(consumer);
	}

	@Override
	public Stream<T> consume(@Nonnull Object key, @Nonnull Observable observable) {
		return (Stream<T>) super.consume(key, observable);
	}

	@Override
	public Stream<T> flush() {
		return (Stream<T>) super.flush();
	}

	@Override
	public <E extends Throwable> Stream<T> when(@Nonnull Class<E> exceptionType, @Nonnull Consumer<E> onError) {
		return (Stream<T>) super.when(exceptionType, onError);
	}

	@Override
	public <V> Stream<V> map(@Nonnull Function<T, V> fn) {
		return (Stream<V>) super.map(fn);
	}

	@Override
	public <V> Stream<V> mapMany(@Nonnull Function<T, Composable<V>> fn) {
		return (Stream<V>) super.mapMany(fn);
	}


	@Override
	public Stream<T> filter(@Nonnull Predicate<T> p) {
		return (Stream<T>) super.filter(p);
	}

	@Override
	public Stream<T> filter(@Nonnull Predicate<T> p, Composable<T> composable) {
		return (Stream<T>) super.filter(p, composable);
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch. Requires a {@code batchSize}
	 * to
	 * have been set.
	 * <p/>
	 * When a new batch is triggered, the first value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @return a new {@code Stream} whose values are the first value of each batch
	 */
	public Stream<T> first() {
		return first(batchSize);
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch. Requires a {@code batchSize}
	 * to
	 * have been set.
	 * <p/>
	 * When a new batch is triggered, the first value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @param batchSize the batch size to use
	 *
	 * @return a new {@code Stream} whose values are the first value of each batch)
	 */
	public Stream<T> first(int batchSize) {
		Assert.state(batchSize > 0, "Cannot first() an unbounded Stream. Try extracting a batch first.");
		final Deferred<T, Stream<T>> d = createDeferredChildStream(batchSize);
		add(new BatchAction<T>(batchSize, getObservable(), null, getError().getT2(), null,
		                       d.compose().getAccept().getT2()));
		return d.compose();
	}

	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code batchSize}
	 *
	 * @return a new {@code Stream} whose values are the last value of each batch
	 */
	public Stream<T> last() {
		return last(batchSize);
	}

	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code batchSize}
	 *
	 * @param batchSize the batch size to use
	 *
	 * @return a new {@code Stream} whose values are the last value of each batch
	 */
	public Stream<T> last(int batchSize) {
		Assert.state(batchSize > 0, "Cannot last() an unbounded Stream. Try extracting a batch first.");
		final Deferred<T, Stream<T>> d = createDeferredChildStream(batchSize);
		add(new BatchAction<T>(batchSize, getObservable(), null, getError().getT2(), d.compose().getAccept().getT2(),
		                       null));
		return d.compose();
	}

	/**
	 * Create a new {@code Stream} whose values will be each element E of any Iterable<E> flowing this Stream
	 * <p/>
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @param <E> the sub element type
	 * @param <T> the enclosing type extending Iterable<E>
	 *
	 * @return a new {@code Stream} whose values result from the iterable input
	 */
	public <E, T extends Iterable<E>> Stream<E> split() {
		final Deferred<E, Stream<E>> d = createDeferred(batchSize);
		getObservable().on(getAccept().getT1(),
				new ForEachAction<T>(getObservable(), d.compose().getAccept().getT2(), getError().getT2()));
		return d.compose();
	}

	/**
	 * Indicates whether or not this {@code Stream} is unbounded.
	 *
	 * @return {@literal true} if a {@code batchSize} has been set, {@literal false} otherwise
	 */
	public boolean isBatch() {
		return batchSize > 0;
	}

	/**
	 * Create a {@link Tap} that maintains a reference to the last value seen by this {@code Stream}. The {@link Tap} is
	 * continually updated when new values pass through the {@code Stream}.
	 *
	 * @return the new {@link Tap}
	 * @see Supplier
	 */
	public Tap<T> tap() {
		final Tap<T> tap = new Tap<T>();
		consume(tap);
		return tap;
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
	 * batchSize} has been reached.
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this batch
	 */
	public Stream<List<T>> collect() {
		Assert.state(batchSize > 0, "Cannot collect() an unbounded Stream. Try extracting a batch first.");
		return collect(batchSize);
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
	 * batchSize} has been reached.
	 *
	 * @param batchSize  the collected size
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this batch
	 */
	public Stream<List<T>> collect(int batchSize) {
		Assert.state(batchSize > 0, "Cannot collect() an unbounded limit.");
		final Deferred<List<T>, Stream<List<T>>> d = createDeferred(batchSize);

		add(new CollectAction<T>(
				batchSize,
				d.compose().getObservable(),
				d.compose().getAccept().getT2(),
				getError().getT2()));

		return d.compose();
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}. The given initial object will be
	 * passed to the function's {@link Tuple2} argument.
	 *
	 * @param fn      the reduce function
	 * @param initial the initial argument to pass to the reduce function
	 * @param <A>     the type of the reduced object
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Stream<A> reduce(@Nonnull Function<Tuple2<T, A>, A> fn, A initial) {
		return reduce(fn, Functions.supplier(initial));
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}. The given {@link Supplier} will be
	 * used to produce initial accumulator objects either on the first reduce call, in the case of an unbounded {@code
	 * Stream}, or on the first value of each batch, if a {@code batchSize} is set.
	 * <p/>
	 * In an unbounded {@code Stream}, the accumulated value will be published on the returned {@code Stream} every time
	 * a
	 * value is accepted. But when a {@code batchSize} has been set and {@link #isBatch()} returns true, the accumulated
	 * value will only be published on the new {@code Stream} at the end of each batch. On the next value (the first of
	 * the next batch), the {@link Supplier} is called again for a new accumulator object and the reduce starts over with
	 * a new accumulator.
	 *
	 * @param fn           the reduce function
	 * @param accumulators the {@link Supplier} that will provide accumulators
	 * @param <A>          the type of the reduced object
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Stream<A> reduce(@Nonnull final Function<Tuple2<T, A>, A> fn, @Nullable final Supplier<A> accumulators) {
		final Deferred<A, Stream<A>> d = createDeferred();
		final Stream<A> stream = d.compose();

		if (isBatch()) {
			add(new ReduceAction<T, A>(
					batchSize,
					accumulators,
					fn,
					stream.getObservable(), stream.getAccept().getT2(), getError().getT2()
			));
		} else {
			add(new ScanAction<T, A>(
					accumulators,
					fn,
					stream.getObservable(), stream.getAccept().getT2(), getError().getT2()
			));
		}

		return d.compose();
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}.
	 *
	 * @param fn  the reduce function
	 * @param <A> the type of the reduced object
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Stream<A> reduce(@Nonnull final Function<Tuple2<T, A>, A> fn) {
		return reduce(fn, (Supplier<A>) null);
	}

	@Override
	protected <V, C extends Composable<V>> Deferred<V, C> createDeferred() {
		return createDeferred(batchSize);
	}

	@SuppressWarnings("unchecked")
	protected <V, C extends Composable<V>> Deferred<V, C> createDeferred(int batchSize) {
		return (Deferred<V, C>) createDeferredChildStream(batchSize);
	}

	private Deferred<T, Stream<T>> createDeferredChildStream() {
		return createDeferredChildStream(-1);
	}

	private Deferred<T, Stream<T>> createDeferredChildStream(int batchSize) {
		Stream<T> stream = new Stream<T>(null,
				batchSize,
				null,
				this);
		return new Deferred<T, Stream<T>>(stream);
	}

}