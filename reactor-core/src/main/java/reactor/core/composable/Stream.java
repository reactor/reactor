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

import reactor.core.Environment;
import reactor.core.Observable;
import reactor.core.action.*;
import reactor.core.composable.spec.DeferredStreamSpec;
import reactor.event.dispatch.Dispatcher;
import reactor.event.selector.Selector;
import reactor.function.*;
import reactor.function.support.Tap;
import reactor.timer.Timer;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A {@code Stream} is a stateless event processor that provides methods for attaching {@link
 * Consumer Consumers} to consume the values passing through the stream. Some methods like
 * {@link #map(reactor.function.Function)}, {@link #filter(reactor.function.Predicate)},
 * {@link #first()}, {@link #last()}, and {@link #reduce(reactor.function.Function, Object)}
 * return new {@code Stream Streams} whose values are the result of transformation functions,
 * filtering, and the like.
 * <p>
 * Typically, new {@code Stream Streams} aren't created directly. To create a {@code Stream},
 * create a {@link DeferredStreamSpec} and configure it with the appropriate {@link Environment},
 * {@link Dispatcher}, and other settings, then call {@link Deferred#compose()}, which will
 * return a {@code Stream} that handles the values passed into the {@link Deferred}.
 * </p>
 *
 * @param <T>
 * 		the type of the values in the stream
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public class Stream<T> extends Composable<T> {

	protected final int         batchSize;
	protected final Environment environment;

	/**
	 * Create a new Stream that will use the {@link Observable} to pass its values to registered
	 * handlers.
	 * <p>
	 * The stream will batch values into batches of the given
	 * {@code batchSize}, affecting the values that are passed to the {@link #first()} and {@link
	 * #last()} substreams. A size of {@code -1} indicates that the stream should not be batched.
	 * </p>
	 * Once all event handler have been registered, {@link #flush} must be called to pass the initial values
	 * to those handlers. The stream will accept errors from the given {@code parent}.
	 *
	 * @param observable
	 * 		The observable used to drive event handlers
	 * @param batchSize
	 * 		The size of the batches, or {@code -1} for no batching
	 * @param parent
	 * 		The stream's parent. May be {@code null}
	 */
	public Stream(@Nullable final Observable observable,
	              int batchSize,
	              @Nullable final Composable<?> parent,
	              @Nullable final Environment environment) {
		this(observable, batchSize, parent, null, environment);
	}

	/**
	 * Create a new Stream that will use the {@link Observable} to pass its values to registered
	 * handlers.
	 * <p>
	 * The stream will batch values into batches of the given {@code batchSize}, affecting the values that are passed to
	 * the {@link #first()} and {@link #last()} substreams. A size of {@code -1} indicates that the stream should not be
	 * batched.
	 * </p>
	 * Once all event handler have been registered, {@link #flush} must be called to pass the initial values
	 * to those handlers. The stream will accept errors from the given {@code parent}.
	 *
	 * @param observable
	 * 		The observable used to drive event handlers
	 * @param batchSize
	 * 		The size of the batches, or {@code -1} for no batching
	 * @param parent
	 * 		The stream's parent. May be {@code null}
	 * @param acceptSelector
	 * 		The tuple Selector/Key to accept values on this observable. May be {@code null}
	 */
	public Stream(@Nullable final Observable observable,
	              int batchSize,
	              @Nullable final Composable<?> parent, Tuple2<Selector, Object> acceptSelector,
	              @Nullable final Environment environment) {
		super(observable, parent, acceptSelector);
		this.batchSize = batchSize;
		this.environment = environment;
	}

	@Override
	public Stream<T> consume(@Nonnull Consumer<T> consumer) {
		return (Stream<T>)super.consume(consumer);
	}

	@Override
	public Stream<T> connect(@Nonnull Composable<T> consumer) {
		return (Stream<T>)super.connect(consumer);
	}

	@Override
	public Stream<T> consume(@Nonnull Object key, @Nonnull Observable observable) {
		return (Stream<T>)super.consume(key, observable);
	}

	@Override
	public Stream<T> flush() {
		return (Stream<T>)super.flush();
	}

	@Override
	public <E extends Throwable> Stream<T> when(@Nonnull Class<E> exceptionType, @Nonnull Consumer<E> onError) {
		return (Stream<T>)super.when(exceptionType, onError);
	}

	@Override
	public <V> Stream<V> map(@Nonnull Function<T, V> fn) {
		return (Stream<V>)super.map(fn);
	}

	@Override
	public <V, C extends Composable<V>> Stream<V> mapMany(@Nonnull Function<T, C> fn) {
		return (Stream<V>)super.mapMany(fn);
	}

	@Override
	public Stream<T> filter(@Nonnull Function<T, Boolean> p) {
		return (Stream<T>)super.filter(p);
	}

	@Override
	public Stream<T> filter(@Nonnull Predicate<T> p) {
		return (Stream<T>)super.filter(p);
	}

	@Override
	public Stream<T> filter(@Nonnull Predicate<T> p, Composable<T> composable) {
		return (Stream<T>)super.filter(p, composable);
	}

	@Override
	public Stream<T> consumeFlush(@Nonnull Flushable<T> consumer) {
		return (Stream<T>)super.consumeFlush(consumer);
	}


	/**
	 * Create a new {@code Stream} whose values will be each iterated item from {@param iterable}
	 * Every time flush is triggered,  {@param iterable} is drained.
	 *
	 * @param iterable
	 * 		the iterable to drain
	 *
	 * @return a new {@code Stream} whose values are the iterated one on flush
	 */
	public Stream<T> propagate(Iterable<T> iterable) {
		consumeFlush(new ForEachAction<T>(iterable,
		                                  batchSize,
		                                  getObservable(),
		                                  getAcceptKey(),
		                                  getError().getObject()));
		return this;
	}

	/**
	 * Create a new {@code Stream} whose values will be generated from {@param supplier}.
	 * Every time flush is triggered, {@param supplier} is called.
	 *
	 * @param supplier
	 * 		the supplier to drain
	 *
	 * @return a new {@code Stream} whose values are generated on each flush
	 */
	public Stream<T> propagate(Supplier<T> supplier) {
		consumeFlush(new SupplyAction<T>(supplier,
		                                 getObservable(),
		                                 getAcceptKey(),
		                                 getError().getObject()));
		return this;
	}

	/**
	 * Consume values and trigger flush when {@param predicate} matches.
	 *
	 * @param predicate
	 * 		the test returning true to trigger flush
	 *
	 * @return the current Stream
	 */
	public Stream<T> flushWhen(Predicate<T> predicate) {
		add(new WhenAction<T>(predicate,
		                      getObservable(),
		                      getFlush().getObject(),
		                      getError().getObject()));
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
	 * @param batchSize
	 * 		the batch size to use
	 *
	 * @return a new {@code Stream} whose values are the first value of each batch)
	 */
	public Stream<T> first(int batchSize) {
		Assert.state(batchSize > 0, "Cannot first() an unbounded Stream. Try extracting a batch first.");
		final Deferred<T, Stream<T>> d = createDeferredChildStream(batchSize);
		add(new BatchAction<T>(batchSize,
		                       getObservable(),
		                       null,
		                       getError().getObject(),
		                       null,
		                       d.compose().getAcceptKey()));
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
	 * @param batchSize
	 * 		the batch size to use
	 *
	 * @return a new {@code Stream} whose values are the last value of each batch
	 */
	public Stream<T> last(int batchSize) {
		Assert.state(batchSize > 0, "Cannot last() an unbounded Stream. Try extracting a batch first.");
		final Deferred<T, Stream<T>> d = createDeferredChildStream(batchSize);
		add(new BatchAction<T>(batchSize,
		                       getObservable(),
		                       null,
		                       getError().getObject(),
		                       d.compose().getAcceptKey(),
		                       null));
		return d.compose();
	}


	/**
	 * Create a new {@code Stream} whose values will be each element E of any Iterable<E> flowing this Stream
	 * <p/>
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @return a new {@code Stream} whose values result from the iterable input
	 */
	public Stream<T> split() {
		return split(batchSize);
	}

	/**
	 * Create a new {@code Stream} whose values will be each element E of any Iterable<E> flowing this Stream
	 * <p/>
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @param batchSize
	 * 		the batch size to use
	 *
	 * @return a new {@code Stream} whose values result from the iterable input
	 */
	public Stream<T> split(int batchSize) {
		final Deferred<T, Stream<T>> d = createDeferred(batchSize);
		getObservable().on(getAcceptSelector(), new ForEachAction<T>(batchSize,
		                                                             getObservable(),
		                                                             d.compose().getAcceptKey(),
		                                                             getError().getObject()));
		return d.compose();
	}

	/**
	 * Create a {@link Tap} that maintains a reference to the last value seen by this {@code Stream}. The {@link Tap} is
	 * continually updated when new values pass through the {@code Stream}.
	 *
	 * @return the new {@link Tap}
	 *
	 * @see Supplier
	 */
	public Tap<T> tap() {
		final Tap<T> tap = new Tap<T>();
		consume(tap);
		return tap;
	}


	/**
	 * Use {@link reactor.core.Observable#batchNotify(Object)} to group dispatching by the current batch size.
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @return a new {@code Stream} whose values result from the iterable input
	 */
	public Stream<T> buffer() {
		return buffer(batchSize);
	}

	/**
	 * Use {@link reactor.core.Observable#batchNotify(Object)} to group dispatch by the passed {@param
	 * batchSize}.
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @param batchSize
	 * 		the batch size to use
	 *
	 * @return a new {@code Stream} whose values result from the iterable input
	 */
	public Stream<T> buffer(int batchSize) {
		final Deferred<T, Stream<T>> d = createDeferred(batchSize);
		add(new BufferAction<T>(batchSize,
		                        d.compose().getObservable(),
		                        d.compose().getAcceptKey(),
		                        getError().getObject()));
		return d.compose();
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed separately into the returned {@code Stream}
	 * every time {@code
	 * batchSize} has been reached. All errors are also captured until current batchSize or flush is called.
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this batch
	 */
	public Stream<List<T>> bufferWithErrors() {
		return bufferWithErrors(batchSize);
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed separately into the returned {@code Stream} every
	 * time {@code
	 * batchSize} has been reached. All errors are also captured until batchSize or flush is called.
	 *
	 * @param batchSize
	 * 		the collected size
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this batch
	 */
	public Stream<List<T>> bufferWithErrors(int batchSize) {
		final Deferred<List<T>, Stream<List<T>>> d = createDeferred(1);

		add(new BufferAction<T>(batchSize,
		                        d.compose().getObservable(),
		                        d.compose().getAcceptKey(),
		                        getError().getObject()));

		getObservable().on(getError(), new BufferAction<Throwable>(batchSize,
		                                                           d.compose().getObservable(),
		                                                           d.compose().getError(),
		                                                           null));

		return d.compose();
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
	 * batchSize} or flush is triggered has been reached.
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this batch
	 */
	public Stream<List<T>> collect() {
		return collect(batchSize);
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
	 * batchSize} has been reached.
	 *
	 * @param batchSize
	 * 		the collected size
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this batch
	 */
	public Stream<List<T>> collect(int batchSize) {
		final Deferred<List<T>, Stream<List<T>>> d = createDeferred(1);

		add(new CollectAction<T>(batchSize,
		                         d.compose().getObservable(),
		                         d.compose().getAcceptKey(),
		                         getError().getObject()));

		return d.compose();
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
	 * batchSize} or {@param timeout} has been reached. Flush also releases the current collected items.
	 *
	 * @param batchSize
	 * 		the collected size
	 * @param timeout
	 * 		the collect timeout in milliseconds
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this batch
	 */
	public Stream<List<T>> collectWithTimeout(int batchSize, long timeout) {
		Assert.state(environment != null, "Cannot use default timer as no environment has been provided to this Stream");
		return collectWithTimeout(batchSize, timeout, environment.getRootTimer());
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
	 * batchSize} or {@param timeout} has been reached. Flush also releases the current collected items.
	 *
	 * @param batchSize
	 * 		the collected size
	 * @param timeout
	 * 		the collect timeout in milliseconds
	 * @param timer
	 * 		Timer to use for observing timeout
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this batch
	 */
	public Stream<List<T>> collectWithTimeout(int batchSize, long timeout, Timer timer) {
		final Deferred<List<T>, Stream<List<T>>> d = createDeferred(1);

		add(new CollectWithTimeoutAction<T>(batchSize,
		                                    d.compose().getObservable(),
		                                    d.compose().getAcceptKey(),
		                                    getError().getObject(),
		                                    timer,
		                                    timeout));

		return d.compose();
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every specified
	 * time from the {@param period} in milliseconds. The window runs on a timer from the stream {@link
	 * this#environment}.
	 *
	 * @param period
	 * 		the time period when each window close and flush the attached consumer
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<List<T>> window(int period) {
		return window(period, TimeUnit.MILLISECONDS);
	}

	/**
	 * Collect incoming values into an internal array, providing a {@link List} that will be pushed into the returned
	 * {@code Stream} every specified {@param period} in milliseconds. The window runs on a timer from the stream {@link
	 * this#environment}. After accepting {@param backlog} of items, every old item will be dropped. Resulting {@link
	 * List} will be at most {@param backlog} items long.
	 *
	 * @param period
	 * 		the time period when each window close and flush the attached consumer
	 * @param backlog
	 * 		maximum amount of items to keep
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<List<T>> movingWindow(int period, int backlog) {
		return movingWindow(period, TimeUnit.MILLISECONDS, backlog);
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every specified
	 * time from the {@param period} and a {@param timeUnit}. The window
	 * runs on a timer from the stream {@link this#environment}.
	 *
	 * @param period
	 * 		the time period when each window close and flush the attached consumer
	 * @param timeUnit
	 * 		the time unit used for the period
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<List<T>> window(int period, TimeUnit timeUnit) {
		return window(period, timeUnit, 0);
	}

	/**
	 * Collect incoming values into an internal array, providing a {@link List} that will be pushed into the returned
	 * {@code Stream} every specified time from the {@param period} and a {@param timeUnit}.
	 *
	 * @param period
	 * 		the time period when each window close and flush the attached consumer
	 * @param timeUnit
	 * 		the time unit used for the period
	 * @param backlog
	 * 		maximum amount of items to keep
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<List<T>> movingWindow(int period, TimeUnit timeUnit, int backlog) {
		return movingWindow(period, timeUnit, 0, backlog);
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every specified
	 * time from the {@param period}, {@param timeUnit} after an initial {@param delay} in milliseconds. The window
	 * runs on a timer from the stream {@link this#environment}.
	 *
	 * @param period
	 * 		the time period when each window close and flush the attached consumer
	 * @param timeUnit
	 * 		the time unit used for the period
	 * @param delay
	 * 		the initial delay in milliseconds
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<List<T>> window(int period, TimeUnit timeUnit, int delay) {
		Assert.state(environment != null, "Cannot use default timer as no environment has been provided to this Stream");
		return window(period, timeUnit, delay, environment.getRootTimer());
	}

	/**
	 * Collect incoming values into an internal array, providing a {@link List} that will be pushed into the returned
	 * {@code Stream} every specified time from the {@param period} and a {@param timeUnit} after an initial {@param
	 * delay}
	 * in milliseconds.
	 *
	 * @param period
	 * 		the time period when each window close and flush the attached consumer
	 * @param timeUnit
	 * 		the time unit used for the period
	 * @param delay
	 * 		the initial delay in milliseconds
	 * @param backlog
	 * 		maximum amount of items to keep
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<List<T>> movingWindow(int period, TimeUnit timeUnit, int delay, int backlog) {
		Assert.state(environment != null, "Cannot use default timer as no environment has been provided to this Stream");
		return movingWindow(period, timeUnit, delay, backlog, environment.getRootTimer());
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every specified
	 * time from the {@param period}, {@param timeUnit} after an initial {@param delay} in milliseconds. The window
	 * runs on a supplied {@param timer}.
	 *
	 * @param period
	 * 		the time period when each window close and flush the attached consumer
	 * @param timeUnit
	 * 		the time unit used for the period
	 * @param delay
	 * 		the initial delay in milliseconds
	 * @param timer
	 * 		the reactor timer to run the window on
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<List<T>> window(int period, TimeUnit timeUnit, int delay, Timer timer) {
		Assert.state(timer != null, "Timer must be supplied");
		final Deferred<List<T>, Stream<List<T>>> d = createDeferred(1);

		add(new WindowAction<T>(d.compose().getObservable(),
		                        d.compose().getAcceptKey(),
		                        getError().getObject(),
		                        timer,
		                        period, timeUnit, delay));

		return d.compose();
	}

	/**
	 * Collect incoming values into an internal array, providing a {@link List} that will be pushed into the returned
	 * {@code Stream} every specified time from the {@param period} and a {@param timeUnit} after an initial {@param
	 * delay}
	 * in milliseconds.
	 *
	 * @param period
	 * 		the time period when each window close and flush the attached consumer
	 * @param timeUnit
	 * 		the time unit used for the period
	 * @param delay
	 * 		the initial delay in milliseconds
	 * @param backlog
	 * 		maximum amount of items to keep
	 * @param timer
	 * 		the reactor timer to run the window on
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<List<T>> movingWindow(int period, TimeUnit timeUnit, int delay, int backlog, Timer timer) {
		Assert.state(timer != null, "Timer must be supplied");
		final Deferred<List<T>, Stream<List<T>>> d = createDeferred(1);

		add(new MovingWindowAction<T>(d.compose().getObservable(),
		                              d.compose().getAcceptKey(),
		                              getError().getObject(),
		                              timer,
		                              period, timeUnit, delay, backlog));

		return d.compose();
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}. The given initial object will be
	 * passed to the function's {@link Tuple2} argument.
	 *
	 * @param fn
	 * 		the reduce function
	 * @param initial
	 * 		the initial argument to pass to the reduce function
	 * @param <A>
	 * 		the type of the reduced object
	 *
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Stream<A> reduce(@Nonnull Function<Tuple2<T, A>, A> fn, A initial) {
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
	 * @param fn
	 * 		the reduce function
	 * @param accumulators
	 * 		the {@link Supplier} that will provide accumulators
	 * @param batchSize
	 * 		the batch size to use
	 * @param <A>
	 * 		the type of the reduced object
	 *
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Stream<A> reduce(@Nonnull final Function<Tuple2<T, A>, A> fn, @Nullable final Supplier<A> accumulators,
	                            final int batchSize
	) {
		final Deferred<A, Stream<A>> d = createDeferred(1);
		final Stream<A> stream = d.compose();
		add(new ReduceAction<T, A>(batchSize,
		                           accumulators,
		                           fn,
		                           stream.getObservable(),
		                           stream.getAcceptKey(),
		                           getError().getObject()));

		return stream;
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}.
	 *
	 * @param fn
	 * 		the reduce function
	 * @param <A>
	 * 		the type of the reduced object
	 *
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Stream<A> reduce(@Nonnull final Function<Tuple2<T, A>, A> fn) {
		return reduce(fn, (Supplier<A>)null, batchSize);
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}. The given initial object will be
	 * passed to the function's {@link Tuple2} argument. Behave like Reduce but triggers downstream Stream for every
	 * transformation.
	 *
	 * @param fn
	 * 		the scan function
	 * @param initial
	 * 		the initial argument to pass to the reduce function
	 * @param <A>
	 * 		the type of the reduced object
	 *
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Stream<A> scan(@Nonnull Function<Tuple2<T, A>, A> fn, A initial) {
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
	 * @param fn
	 * 		the scan function
	 * @param accumulators
	 * 		the {@link Supplier} that will provide accumulators
	 * @param <A>
	 * 		the type of the reduced object
	 *
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Stream<A> scan(@Nonnull final Function<Tuple2<T, A>, A> fn, @Nullable final Supplier<A> accumulators) {
		final Deferred<A, Stream<A>> d = createDeferred(1);
		final Stream<A> stream = d.compose();
		add(new ScanAction<T, A>(accumulators,
		                         fn,
		                         stream.getObservable(),
		                         stream.getAcceptKey(),
		                         getError().getObject()));

		return stream;
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}.
	 *
	 * @param fn
	 * 		the reduce function
	 * @param <A>
	 * 		the type of the reduced object
	 *
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Stream<A> scan(@Nonnull final Function<Tuple2<T, A>, A> fn) {
		return scan(fn, (Supplier<A>)null);
	}

	@Override
	protected <V, C extends Composable<V>> Deferred<V, C> createDeferred() {
		return createDeferred(batchSize);
	}

	protected <V, C extends Composable<V>> Deferred<V, C> createDeferred(int batchSize) {
		return createDeferredChildStream(batchSize);
	}

	BufferAction<T> bufferConsumer(int batchSize) {
		BufferAction<T> bufferAction = new BufferAction<T>(batchSize, getObservable(), getAcceptKey(), getError());
		consumeFlush(bufferAction);
		return bufferAction;
	}

	@SuppressWarnings("unchecked")
	private <V, C extends Composable<V>> Deferred<V, C> createDeferredChildStream(int batchSize) {
		C stream = (C)new Stream<V>(null,
		                            batchSize,
		                            this,
		                            environment);

		return new Deferred<V, C>(stream);
	}

}