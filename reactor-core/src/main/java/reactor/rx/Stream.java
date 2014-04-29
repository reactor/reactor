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

package reactor.rx;

import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.block.procedure.checked.CheckedProcedure;
import com.gs.collections.impl.list.mutable.MultiReaderFastList;
import org.reactivestreams.spi.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.alloc.Recyclable;
import reactor.core.Environment;
import reactor.core.Observable;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.selector.Selectors;
import reactor.function.*;
import reactor.function.support.Tap;
import reactor.rx.action.*;
import reactor.timer.Timer;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Base class for components designed to provide a succinct API for working with future values.
 * Provides base functionality and an internal contract for subclasses that make use of
 * the {@link #map(reactor.function.Function)} and {@link #filter(reactor.function.Predicate)} methods.
 * <p/>
 * A Stream can be implemented to perform specific actions on callbacks (doNext,doComplete,doError,doSubscribe).
 * It is an asynchronous boundary and will run the callbacks using the input {@link Dispatcher}. Stream can
 * eventually
 * produce a result {@param <O>} and will offer cascading over its own subscribers.
 * <p/>
 * * <p>
 * Typically, new {@code Stream Streams} aren't created directly. To create a {@code Stream},
 * create a {@link reactor.rx.spec.Streams} and configure it with the appropriate {@link Environment},
 * {@link Dispatcher}, and other settings.
 * </p>
 *
 * @param <O> The type of the output values
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1
 */
public class Stream<O> implements Pipeline<O>, Recyclable {

	private static final Logger log = LoggerFactory.getLogger(Stream.class);

	private final MultiReaderFastList<StreamSubscription<O>> subscriptions   = MultiReaderFastList.newList(8);

	protected final Dispatcher dispatcher;

	protected Throwable error = null;
	protected boolean   pause = false;
	protected int batchSize;

	protected boolean keepAlive = false;
	private Environment environment;
	private State state = State.READY;

	public Stream() {
		this(SynchronousDispatcher.INSTANCE);
	}

	public Stream(Dispatcher dispatcher) {
		this(dispatcher, Integer.MAX_VALUE);
	}

	public Stream(Dispatcher dispatcher, int batchSize) {
		this(dispatcher, null, batchSize);
	}

	public Stream(Dispatcher dispatcher,
	              @Nullable Environment environment,
	              int batchSize) {
		Assert.state(dispatcher != null, "'dispatcher'  cannot be null.");
		this.environment = environment;
		this.batchSize = batchSize;
		this.dispatcher = dispatcher;
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
	public <E extends Throwable> Stream<O> when(@Nonnull final Class<E> exceptionType,
	                                            @Nonnull final Consumer<E> onError) {
		connect(new ErrorAction<O, E>(dispatcher, Selectors.T(exceptionType), onError));
		return this;
	}

	@Override
	public <E> Stream<E> connect(@Nonnull final Action<O, E> stream) {
		stream.prefetch(batchSize).env(environment);
		stream.setKeepAlive(keepAlive);
		this.subscribe(stream);
		return stream;
	}

	public Stream<O> prefetch(int elements) {
		this.batchSize = elements;
		return this;
	}

	public Stream<O> env(Environment environment) {
		this.environment = environment;
		return this;
	}

	/**
	 * Materialize an error state into a downstream event.
	 *
	 * @param exceptionType the type of exceptions to handle
	 * @param <E>           type of the exception to handle
	 * @return {@literal this}
	 */
	public <E extends Throwable> Stream<E> recover(@Nonnull final Class<E> exceptionType) {
		RecoverAction<O, E> recoverAction = new RecoverAction<O, E>(dispatcher, Selectors.T(exceptionType));
		return connect(recoverAction);
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will consume any values accepted by this {@code
	 * Stream}.
	 *
	 * @param consumer the conumer to invoke on each value
	 * @return {@literal this}
	 */
	public Stream<O> consume(@Nonnull final Consumer<O> consumer) {
		connect(new CallbackAction<O>(dispatcher, consumer, true));
		return this;
	}

	/**
	 * Pass values accepted by this {@code Stream} into the given {@link Observable}, notifying with the given key.
	 *
	 * @param key        the key to notify on
	 * @param observable the {@link Observable} to notify
	 * @return {@literal this}
	 */
	public Stream<O> consume(@Nonnull final Object key, @Nonnull final Observable observable) {
		connect(new ObservableAction<O>(dispatcher, observable, key));
		return this;
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code V} and pass it into
	 * another {@code Stream}.
	 *
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@code Stream} containing the transformed values
	 */
	public <V> Stream<V> map(@Nonnull final Function<O, V> fn) {
		final MapAction<O, V> d = new MapAction<O, V>(fn, dispatcher);
		return connect(d);
	}

	/**
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@code Stream} containing the transformed values
	 * @see {@link org.reactivestreams.api.Producer#produceTo(org.reactivestreams.api.Consumer)}
	 * @since 1.1
	 */
	public <V, C extends Stream<V>> Stream<V> flatMap(@Nonnull final Function<O, C> fn) {
		return mapMany(fn);
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code Stream<O,V>} and pass
	 * it into another {@code Stream}.
	 *
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@code Stream} containing the transformed values
	 * @since 1.1
	 */
	public <V, C extends Stream<V>> Stream<V> mapMany(@Nonnull final Function<O, C> fn) {
		final MapManyAction<O, V, C> d = new MapManyAction<O, V, C>(fn, dispatcher);
		connect(d);
		return d.mergedStream();
	}

	/**
	 * {@link this#connect(Action)} all the passed {@param composables} to this {@link Stream},
	 * merging values streams into the current pipeline.
	 *
	 * @param composables the the composables to connect
	 * @return the merged stream
	 * @since 1.1
	 */
	@SuppressWarnings("unchecked")
	public Stream<O> merge(Stream<O>... composables) {
		Stream<O>[] thisAndComposable = new Stream[composables.length+1];
		thisAndComposable[0] = this;
		System.arraycopy(composables,0, thisAndComposable,1, composables.length);

		final MergeAction<O> mergeAction = new MergeAction<O>(dispatcher, null, thisAndComposable);
		return connect(mergeAction);
	}

	/**
	 * Attach a No-Op Action that only serves the purpose of buffering incoming values if no subscriber is attached
	 * downstream.
	 *
	 * @return a buffered stream
	 * @since 1.1
	 */
	public Stream<O> buffer() {
		return connect(new Action<O, O>(dispatcher, batchSize){
			@Override
			protected void doNext(O ev) {
				this.broadcastNext(ev);
			}
		});
	}

	/**
	 * Evaluate each accepted value against the given predicate {@link Function}. If the predicate test succeeds, the
	 * value is passed into the new {@code Stream}. If the predicate test fails, an exception is propagated into the
	 * new {@code Stream}.
	 *
	 * @param fn the predicate {@link Function} to test values against
	 * @return a new {@code Stream} containing only values that pass the predicate test
	 */
	public FilterAction<O, Stream<O>> filter(@Nonnull final Function<O, Boolean> fn) {
		final FilterAction<O, Stream<O>> d = new FilterAction<O, Stream<O>>(fn, dispatcher);
		connect(d);
		return d;
	}

	/**
	 * Evaluate each accepted value against the given {@link Predicate}. If the predicate test succeeds, the value is
	 * passed into the new {@code Stream}. If the predicate test fails, the value is propagated into the {@link
	 * reactor.rx.action.FilterAction#otherwise()} composable .
	 *
	 * @param p the {@link Predicate} to test values against
	 * @return a new {@code Stream} containing only values that pass the predicate test
	 */
	public FilterAction<O, Stream<O>> filter(final Predicate<O> p) {
		final FilterAction<O, Stream<O>> d = new FilterAction<O, Stream<O>>(p, dispatcher);
		connect(d);
		return d;
	}

	/**
	 * Evaluate each accepted boolean value. If the predicate test succeeds, the value is
	 * passed into the new {@code Stream}. If the predicate test fails, the value is ignored.
	 *
	 * @return a new {@code Stream} containing only values that pass the predicate test
	 * @since 1.1
	 */
	@SuppressWarnings("unchecked")
	public FilterAction<Boolean, Stream<Boolean>> filter() {
		return ((Stream<Boolean>) this).filter(FilterAction.simplePredicate);
	}

	/**
	 * Create a new {@code Stream} whose values will be generated from {@param supplier}.
	 * Every time flush is triggered, {@param supplier} is called.
	 *
	 * @param supplier the supplier to drain
	 * @return a new {@code Stream} whose values are generated on each flush
	 * @since 1.1
	 */
	public <E> Stream<E> propagate(final Supplier<E> supplier) {
		final SupplierAction<O, E> d = new SupplierAction<O, E>(dispatcher, supplier);
		d.env(environment).setKeepAlive(keepAlive);
		subscribe(d);
		return d;
	}

	/**
	 * Consume values and trigger flush when {@param predicate} matches.
	 *
	 * @param predicate the test returning true to trigger flush
	 * @return the current Stream
	 * @since 1.1
	 */
	public Stream<O> flushWhen(Predicate<O> predicate) {
		connect(new FlushWhenAction<O>(predicate, dispatcher, this));
		return this;
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
	public Stream<O> first() {
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
	 * @return a new {@code Stream} whose values are the first value of each batch)
	 */
	public Stream<O> first(int batchSize) {
		final FirstAction<O> d = new FirstAction<O>(batchSize, dispatcher);
		d.env(environment).setKeepAlive(keepAlive);
		subscribe(d);
		return d;
	}

	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code batchSize}
	 *
	 * @return a new {@code Stream} whose values are the last value of each batch
	 */
	public Stream<O> last() {
		return last(batchSize);
	}

	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code batchSize}
	 *
	 * @param batchSize the batch size to use
	 * @return a new {@code Stream} whose values are the last value of each batch
	 */
	public Stream<O> last(int batchSize) {
		final LastAction<O> d = new LastAction<O>(batchSize, dispatcher);
		d.env(environment).setKeepAlive(keepAlive);
		subscribe(d);
		return d;
	}


	/**
	 * Create a new {@code Stream} that filters out consecutive equals values.
	 *
	 * @return a new {@code Stream} whose values are the last value of each batch
	 * @since 1.1
	 */
	public Stream<O> distinctUntilChanged() {
		final DistinctUntilChangedAction<O> d = new DistinctUntilChangedAction<O>(dispatcher);
		return connect(d);
	}


	/**
	 * Create a new {@code Stream} whose values will be each element E of any Iterable<E> flowing this Stream
	 * <p/>
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @return a new {@code Stream} whose values result from the iterable input
	 * @since 1.1
	 */
	public <V> Stream<V> split() {
		return split(batchSize);
	}

	/**
	 * Create a new {@code Stream} whose values will be each element E of any Iterable<E> flowing this Stream
	 * <p/>
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @param batchSize the batch size to use
	 * @return a new {@code Stream} whose values result from the iterable input
	 * @since 1.1
	 */
	@SuppressWarnings("unchecked")
	public <V> Stream<V> split(int batchSize) {
		final ForEachAction<V> d = new ForEachAction<V>(dispatcher);
		final Stream<Iterable<V>> iterableStream = (Stream<Iterable<V>>) this;
		d.prefetch(batchSize).env(environment).setKeepAlive(keepAlive);
		iterableStream.subscribe(d);
		return d;
	}

	/**
	 * Create a {@link reactor.function.support.Tap} that maintains a reference to the last value seen by this {@code
	 * Stream}. The {@link reactor.function.support.Tap} is
	 * continually updated when new values pass through the {@code Stream}.
	 *
	 * @return the new {@link reactor.function.support.Tap}
	 * @see Consumer
	 */
	public Tap<O> tap() {
		final Tap<O> tap = new Tap<O>();
		connect(new CallbackAction<O>(dispatcher, tap, true));
		return tap;
	}

	/**
	 * Collect incoming values into a {@link java.util.List} that will be pushed into the returned {@code Stream} every
	 * time {@code
	 * batchSize} or flush is triggered has been reached.
	 *
	 * @return a new {@code Stream} whose values are a {@link java.util.List} of all values in this batch
	 */
	public Stream<List<O>> collect() {
		return collect(batchSize);
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
	 * batchSize} has been reached.
	 *
	 * @param batchSize the collected size
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this batch
	 */
	public CollectAction<O> collect(int batchSize) {
		final CollectAction<O> d = new CollectAction<O>(batchSize, dispatcher);
		d.env(environment).setKeepAlive(keepAlive);
		subscribe(d);
		return d;
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every specified
	 * time from the {@param period} in milliseconds. The window runs on a timer from the stream {@link
	 * this#environment}.
	 *
	 * @param period the time period when each window close and flush the attached consumer
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 * @since 1.1
	 */
	public Stream<List<O>> window(int period) {
		return window(period, TimeUnit.MILLISECONDS);
	}

	/**
	 * Collect incoming values into an internal array, providing a {@link List} that will be pushed into the returned
	 * {@code Stream} every specified {@param period} in milliseconds. The window runs on a timer from the stream {@link
	 * this#environment}. After accepting {@param backlog} of items, every old item will be dropped. Resulting {@link
	 * List} will be at most {@param backlog} items long.
	 *
	 * @param period  the time period when each window close and flush the attached consumer
	 * @param backlog maximum amount of items to keep
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 * @since 1.1
	 */
	public Stream<List<O>> movingWindow(int period, int backlog) {
		return movingWindow(period, TimeUnit.MILLISECONDS, backlog);
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every specified
	 * time from the {@param period} and a {@param timeUnit}. The window
	 * runs on a timer from the stream {@link this#environment}.
	 *
	 * @param period   the time period when each window close and flush the attached consumer
	 * @param timeUnit the time unit used for the period
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 * @since 1.1
	 */
	public Stream<List<O>> window(int period, TimeUnit timeUnit) {
		return window(period, timeUnit, 0);
	}

	/**
	 * Collect incoming values into an internal array, providing a {@link List} that will be pushed into the returned
	 * {@code Stream} every specified time from the {@param period} and a {@param timeUnit}.
	 *
	 * @param period   the time period when each window close and flush the attached consumer
	 * @param timeUnit the time unit used for the period
	 * @param backlog  maximum amount of items to keep
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 * @since 1.1
	 */
	public Stream<List<O>> movingWindow(int period, TimeUnit timeUnit, int backlog) {
		return movingWindow(period, timeUnit, 0, backlog);
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every specified
	 * time from the {@param period}, {@param timeUnit} after an initial {@param delay} in milliseconds. The window
	 * runs on a timer from the stream {@link this#environment}.
	 *
	 * @param period   the time period when each window close and flush the attached consumer
	 * @param timeUnit the time unit used for the period
	 * @param delay    the initial delay in milliseconds
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 * @since 1.1
	 */
	public Stream<List<O>> window(int period, TimeUnit timeUnit, int delay) {
		Assert.state(getEnvironment() != null,
				"Cannot use default timer as no environment has been provided to this Stream");
		return window(period, timeUnit, delay, getEnvironment().getRootTimer());
	}

	/**
	 * Collect incoming values into an internal array, providing a {@link List} that will be pushed into the returned
	 * {@code Stream} every specified time from the {@param period} and a {@param timeUnit} after an initial {@param
	 * delay}
	 * in milliseconds.
	 *
	 * @param period   the time period when each window close and flush the attached consumer
	 * @param timeUnit the time unit used for the period
	 * @param delay    the initial delay in milliseconds
	 * @param backlog  maximum amount of items to keep
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 * @since 1.1
	 */
	public Stream<List<O>> movingWindow(int period, TimeUnit timeUnit, int delay, int backlog) {
		Assert.state(getEnvironment() != null,
				"Cannot use default timer as no environment has been provided to this Stream");
		return movingWindow(period, timeUnit, delay, backlog, getEnvironment().getRootTimer());
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every specified
	 * time from the {@param period}, {@param timeUnit} after an initial {@param delay} in milliseconds. The window
	 * runs on a supplied {@param timer}.
	 *
	 * @param period   the time period when each window close and flush the attached consumer
	 * @param timeUnit the time unit used for the period
	 * @param delay    the initial delay in milliseconds
	 * @param timer    the reactor timer to run the window on
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 * @since 1.1
	 */
	public Stream<List<O>> window(int period, TimeUnit timeUnit, int delay, Timer timer) {
		final Action<O, List<O>> d = new WindowAction<O>(
				dispatcher,
				timer,
				period, timeUnit, delay);
		return connect(d);
	}

	/**
	 * Collect incoming values into an internal array, providing a {@link List} that will be pushed into the returned
	 * {@code Stream} every specified time from the {@param period} and a {@param timeUnit} after an initial {@param
	 * delay}
	 * in milliseconds.
	 *
	 * @param period   the time period when each window close and flush the attached consumer
	 * @param timeUnit the time unit used for the period
	 * @param delay    the initial delay in milliseconds
	 * @param backlog  maximum amount of items to keep
	 * @param timer    the reactor timer to run the window on
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 * @since 1.1
	 */
	public Stream<List<O>> movingWindow(int period, TimeUnit timeUnit, int delay, int backlog, Timer timer) {
		final Action<O, List<O>> d = new MovingWindowAction<O>(
				dispatcher,
				timer,
				period, timeUnit, delay, backlog);
		d.prefetch(backlog).env(environment).setKeepAlive(keepAlive);
		subscribe(d);
		return d;
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}. The given initial object will be
	 * passed to the function's {@link reactor.tuple.Tuple2} argument.
	 *
	 * @param fn      the reduce function
	 * @param initial the initial argument to pass to the reduce function
	 * @param <A>     the type of the reduced object
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Action<O, A> reduce(@Nonnull Function<Tuple2<O, A>, A> fn, A initial) {
		return reduce(fn, Functions.supplier(initial), batchSize);
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}. The given {@link Supplier} will be
	 * used to produce initial accumulator objects either on the first reduce call, in the case of an unbounded {@code
	 * Stream}, or on the first value of each batch, if a {@code batchSize} is set.
	 * <p/>
	 * In an unbounded {@code Stream}, the accumulated value will be published on the returned {@code Stream} on flush
	 * only. But when a {@code batchSize} has been, the accumulated
	 * value will only be published on the new {@code Stream} at the end of each batch. On the next value (the first of
	 * the next batch), the {@link Supplier} is called again for a new accumulator object and the reduce starts over with
	 * a new accumulator.
	 *
	 * @param fn           the reduce function
	 * @param accumulators the {@link Supplier} that will provide accumulators
	 * @param batchSize    the batch size to use
	 * @param <A>          the type of the reduced object
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Action<O, A> reduce(@Nonnull final Function<Tuple2<O, A>, A> fn, @Nullable final Supplier<A> accumulators,
	                               final int batchSize
	) {
		final Action<O, A> stream = new ReduceAction<O, A>(batchSize,
				accumulators,
				fn,
				dispatcher
		);
		stream.env(environment).setKeepAlive(keepAlive);
		subscribe(stream);
		return stream;
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}.
	 *
	 * @param fn  the reduce function
	 * @param <A> the type of the reduced object
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Action<O, A> reduce(@Nonnull final Function<Tuple2<O, A>, A> fn) {
		return reduce(fn, null, batchSize);
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}. The given initial object will be
	 * passed to the function's {@link Tuple2} argument. Behave like Reduce but triggers downstream Stream for every
	 * transformation.
	 *
	 * @param fn      the scan function
	 * @param initial the initial argument to pass to the reduce function
	 * @param <A>     the type of the reduced object
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 * @since 1.1
	 */
	public <A> Action<O, A> scan(@Nonnull Function<Tuple2<O, A>, A> fn, A initial) {
		return scan(fn, Functions.supplier(initial));
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}. The given {@link Supplier} will be
	 * used to produce initial accumulator objects either on the first reduce call, in the case of an unbounded {@code
	 * Stream}, or on the first value of each batch, if a {@code batchSize} is set.
	 * <p/>
	 * The accumulated value will be published on the returned {@code Stream} every time
	 * a
	 * value is accepted.
	 *
	 * @param fn           the scan function
	 * @param accumulators the {@link Supplier} that will provide accumulators
	 * @param <A>          the type of the reduced object
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 * @since 1.1
	 */
	public <A> Action<O, A> scan(@Nonnull final Function<Tuple2<O, A>, A> fn, @Nullable final Supplier<A> accumulators) {
		final Action<O, A> stream = new ScanAction<O, A>(accumulators,
				fn,
				dispatcher);
		connect(stream);
		return stream;
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}.
	 *
	 * @param fn  the reduce function
	 * @param <A> the type of the reduced object
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 * @since 1.1
	 */
	public <A> Action<O, A> scan(@Nonnull final Function<Tuple2<O, A>, A> fn) {
		return scan(fn, (Supplier<A>) null);
	}

	/**
	 * Count accepted events for each batch (every flush) and pass each accumulated long to the {@param stream}.
	 *
	 * @since 1.1
	 */
	public CountAction<O> count() {
		final CountAction<O> countAction = new CountAction<O>(dispatcher);
		connect(countAction);
		return countAction;
	}

	/**
	 * Print a debugged form of the root composable relative to this. The output will be an acyclic directed graph of
	 * composed actions.
	 *
	 * @since 1.1
	 */
	@SuppressWarnings("unchecked")
	public String debug() {
		return StreamUtils.browse(this);
	}

	@Override
	public Stream<O> cancel() {
		state = State.SHUTDOWN;
		return this;
	}

	@Override
	public Stream<O> pause() {
		pause = true;
		return this;
	}

	@Override
	public Stream<O> resume() {
		pause = false;
		return this;
	}

	@Override
	public void recycle() {
		subscriptions.clear();

		state = State.READY;
		environment = null;
		keepAlive = false;
		error = null;
	}

	@Override
	public void produceTo(org.reactivestreams.api.Consumer<O> consumer) {
		subscribe(consumer.getSubscriber());
	}


	@Override
	public void subscribe(final Subscriber<O> subscriber) {
		log.info(this.getClass().getSimpleName() + " > OUT onSubscribe: " + this);

		final StreamSubscription<O> subscription = createSubscription(subscriber);

		if (checkState() && addSubscription(subscription)) {
			subscriber.onSubscribe(subscription);
		} else if (state == State.COMPLETE) {
			subscriber.onComplete();
		} else if (state == State.SHUTDOWN) {
			subscriber.onError(new IllegalStateException("Publisher has shutdown"));
		} else if (state == State.ERROR) {
			subscriber.onError(error);
		}

	}

	protected StreamSubscription<O> createSubscription(Subscriber<O> subscriber) {
		return new StreamSubscription<O>(this, subscriber);
	}

	@Override
	public void broadcastNext(final O ev) {
		try {
			if (!checkState()) return;

			log.info(this.getClass().getSimpleName() + " > OUT onNext:" + ev + " - " + this);

			MutableList<StreamSubscription<O>> list = subscriptions.select(new com.gs.collections.api.block.predicate
					.Predicate<StreamSubscription<O>>() {
				@Override
				public boolean accept(StreamSubscription<O> oStreamSubscription) {
					return !oStreamSubscription.terminated;
				}
			});

			list.forEach(new CheckedProcedure<StreamSubscription<O>>() {
				@Override
				public void safeValue(StreamSubscription<O> subscription) throws Exception {
					try {
						subscription.onNext(ev);
						if (subscription.terminated || state == State.COMPLETE) {
							subscription.onComplete();
						}
					} catch (Throwable throwable) {
						callError(subscription, throwable);
					}
				}
			});

		} catch (final Throwable unrecoverable) {
			error = unrecoverable;
			state = State.ERROR;

			subscriptions.forEach(new CheckedProcedure<StreamSubscription<O>>() {
				@Override
				public void safeValue(StreamSubscription<O> subscription) throws Exception {
					callError(subscription, unrecoverable);
				}
			});
		}

	}

	@Override
	public void broadcastFlush() {
		if (!checkState()) return;

		log.info(this.getClass().getSimpleName() + " > OUT onFlush:" + this);

		subscriptions.select(new com.gs.collections.api.block.predicate.Predicate<StreamSubscription<O>>() {
			@Override
			public boolean accept(StreamSubscription<O> each) {
				return Flushable.class.isAssignableFrom(each.subscriber.getClass());
			}
		}).forEach(new CheckedProcedure<StreamSubscription<O>>() {
			@Override
			public void safeValue(final StreamSubscription<O> object) throws Exception {
				try {
					((Flushable) object.subscriber).onFlush();
				} catch (final Throwable throwable) {
					subscriptions.select(new com.gs.collections.api.block.predicate.Predicate<StreamSubscription<O>>() {
						@Override
						public boolean accept(StreamSubscription<O> each) {
							return each.subscriber == object;
						}
					}).forEach(new CheckedProcedure<StreamSubscription<O>>() {
						@Override
						public void safeValue(StreamSubscription<O> subscription) throws Exception {
							callError(subscription, throwable);
						}
					});
				}
			}
		});
	}

	@Override
	public void broadcastError(final Throwable throwable) {
		if (!checkState()) return;

		log.error(this.getClass().getSimpleName() + " > OUT onError:" + this, new Exception(throwable));

		state = State.ERROR;
		error = throwable;

		if (subscriptions.isEmpty()) return;

		subscriptions.forEach(new CheckedProcedure<StreamSubscription<O>>() {
			@Override
			public void safeValue(StreamSubscription<O> subscription) throws Exception {
				callError(subscription, throwable);
			}
		});
	}

	@Override
	public void broadcastComplete() {
		if (!checkState()) return;

		if (subscriptions.isEmpty()) {
			state = State.COMPLETE;
			return;
		}

		log.info(this.getClass().getSimpleName() + " > OUT onComplete:" + this);

		subscriptions.forEach(new CheckedProcedure<StreamSubscription<O>>() {
			@Override
			public void safeValue(StreamSubscription<O> subscription) throws Exception {
				try {
					subscription.onComplete();
				} catch (Throwable throwable) {
					callError(subscription, throwable);
				}
			}
		});

		state = State.COMPLETE;
	}

	public MutableList<StreamSubscription<O>> getSubscriptions() {
		return subscriptions;
	}

	public void setKeepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
	}

	public State getState() {
		return state;
	}

	public Throwable getError() {
		return error;
	}

	/**
	 * Return defined {@link Stream} batchSize, used to drive new {@link org.reactivestreams.spi.Subscription}
	 * request needs.
	 *
	 * @return
	 */
	public int getBatchSize() {
		return batchSize;
	}

	protected boolean addSubscription(final StreamSubscription<O> subscription) {
		if (subscriptions.indexOf(subscription) != -1) {
			subscription.subscriber.onError(new IllegalStateException("Subscription already exists between this " +
					"publisher/subscriber pair"));
			return false;
		} else {
			return subscriptions.add(subscription);
		}
	}

	protected void removeSubscription(final StreamSubscription<O> subscription) {
		subscriptions.remove(subscription);
		subscriptions.forEach(new CheckedProcedure<StreamSubscription<O>>() {
			@Override
			public void safeValue(StreamSubscription<O> oStreamSubscription) throws Exception {
				long capacity = oStreamSubscription.capacity.get();
				if(capacity > 0){
					oStreamSubscription.requestMore(batchSize > capacity ? batchSize : (int)capacity);
				}
			}
		});
		if (subscriptions.isEmpty() && !keepAlive) {
			state = State.SHUTDOWN;
		}
	}

	protected void setState(State state) {
		this.state = state;
	}

	@Override
	public Stream<O> getPublisher() {
		return this;
	}


	private void callError(StreamSubscription<O> subscription, Throwable cause) {
		subscription.subscriber.onError(cause);
	}

	protected boolean checkState() {
		return state != State.ERROR && state != State.COMPLETE && state != State.SHUTDOWN;

	}

	protected static enum State {
		READY,
		ERROR,
		COMPLETE,
		SHUTDOWN;
	}

	@Override
	public String toString() {
		return "Stream{" +
				"state=" + state +
				", keepAlive=" + keepAlive +
				'}';
	}

	/**
	 * Get the assigned {@link reactor.event.dispatch.Dispatcher}.
	 *
	 * @return
	 */
	public Dispatcher getDispatcher() {
		return dispatcher;
	}


	/**
	 * Get the assigned {@link reactor.core.Environment}.
	 *
	 * @return
	 */
	public Environment getEnvironment() {
		return environment;
	}
}
