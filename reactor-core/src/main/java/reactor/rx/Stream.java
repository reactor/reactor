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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.alloc.Recyclable;
import reactor.core.Environment;
import reactor.core.Observable;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.routing.ArgumentConvertingConsumerInvoker;
import reactor.event.routing.ConsumerFilteringRouter;
import reactor.event.routing.Router;
import reactor.event.selector.Selectors;
import reactor.filter.PassThroughFilter;
import reactor.function.*;
import reactor.function.support.Tap;
import reactor.queue.CompletableQueue;
import reactor.rx.action.*;
import reactor.rx.action.support.GroupedByStream;
import reactor.timer.Timer;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Base class for components designed to provide a succinct API for working with future values.
 * Provides base functionality and an internal contract for subclasses that make use of
 * the {@link #map(reactor.function.Function)} and {@link #filter(reactor.function.Predicate)} methods.
 * 
 * A Stream can be implemented to perform specific actions on callbacks (doNext,doComplete,doError,doSubscribe).
 * It is an asynchronous boundary and will run the callbacks using the input {@link Dispatcher}. Stream can
 * eventually produce a result {@param <O>} and will offer cascading over its own subscribers.
 * 
 * * 
 * Typically, new {@code Stream Streams} aren't created directly. To create a {@code Stream},
 * create a {@link reactor.rx.spec.Streams} and configure it with the appropriate {@link Environment},
 * {@link Dispatcher}, and other settings.
 * 
 *
 * @param <O> The type of the output values
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1, 2.0
 */
public class Stream<O> implements Pipeline<O>, Recyclable {

	protected static final Logger log = LoggerFactory.getLogger(Stream.class);

	public static final Router ROUTER = new ConsumerFilteringRouter(
			new PassThroughFilter(), new ArgumentConvertingConsumerInvoker(null)
	);

	private StreamSubscription<O> downstreamSubscription;

	protected final Dispatcher dispatcher;

	protected Throwable error = null;
	protected boolean   pause = false;
	protected int batchSize;

	protected boolean keepAlive = true;
	protected Environment environment;
	protected State state = State.READY;

	public Stream() {
		this(Integer.MAX_VALUE);

	}

	public Stream(int batchSize) {
		this.dispatcher = SynchronousDispatcher.INSTANCE;
		this.batchSize = batchSize;
	}

	public Stream(Dispatcher dispatcher) {
		this(dispatcher, dispatcher.backlogSize());
	}

	public Stream(Dispatcher dispatcher, int batchSize) {
		this(dispatcher, null, batchSize);
	}

	public Stream(@Nonnull Dispatcher dispatcher,
	              @Nullable Environment environment,
	              int batchSize) {
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
		ErrorAction<O, E> errorAction = new ErrorAction<O, E>(dispatcher, Selectors.T(exceptionType), onError);
		checkAndSubscribe(
				errorAction,
				new StreamSubscription.Firehose<O>(this, errorAction));
		return this;
	}

	@Override
	public <A, E extends Action<O, A>> E connect(@Nonnull final E stream) {
		stream.capacity(batchSize).env(environment);
		stream.setKeepAlive(keepAlive);
		this.subscribe(stream);
		return stream;
	}

	public Stream<O> capacity(int elements) {
		this.batchSize = elements > (dispatcher.backlogSize() - Action.RESERVED_SLOTS) ?
				dispatcher.backlogSize() - Action.RESERVED_SLOTS : elements;
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
	 * @since 2.0
	 */
	public <E extends Throwable> Stream<E> recover(@Nonnull final Class<E> exceptionType) {
		RecoverAction<O, E> recoverAction = new RecoverAction<O, E>(dispatcher, Selectors.T(exceptionType));
		return connect(recoverAction);
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will consume any values accepted by this {@code
	 * Stream}. It will eagerly prefetch upstream publisher, for a passive version
	 * see {@link this#observe(reactor.function.Consumer)}
	 *
	 * @param consumer the conumer to invoke on each value
	 * @return {@literal this}
	 */
	public CallbackAction<O> consume(@Nonnull final Consumer<O> consumer) {
		return connect(new CallbackAction<O>(dispatcher, consumer, true));
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will observe any values accepted by this {@code
	 * Stream}.
	 *
	 * @param consumer the conumer to invoke on each value
	 * @return {@literal this}
	 * @since 2.0
	 */
	public CallbackAction<O> observe(@Nonnull final Consumer<O> consumer) {
		return connect(new CallbackAction<O>(dispatcher, consumer, false));
	}

	/**
	 * Pass values accepted by this {@code Stream} into the given {@link Observable}, notifying with the given key.
	 *
	 * @param key        the key to notify on
	 * @param observable the {@link Observable} to notify
	 * @return {@literal this}
	 * @since 1.1, 2.0
	 */
	public ObservableAction<O> notify(@Nonnull final Object key, @Nonnull final Observable observable) {
		return connect(new ObservableAction<O>(dispatcher, observable, key));
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code V} and pass it into
	 * another {@code Stream}.
	 *
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@code Stream} containing the transformed values
	 */
	public <V> Action<O, V> map(@Nonnull final Function<O, V> fn) {
		final MapAction<O, V> d = new MapAction<O, V>(fn, dispatcher);
		return connect(d);
	}

	/**
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@code Stream} containing the transformed values
	 * @since 2.0
	 */
	public <V, C extends Publisher<V>> MapManyAction<O, V, C> flatMap(@Nonnull final Function<O, C> fn) {
		return mapMany(fn);
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code Stream<O,V>} and pass
	 * it into another {@code Stream}.
	 *
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@code Stream} containing the transformed values
	 * @since 1.1, 2.0
	 */
	public <V, C extends Publisher<V>> MapManyAction<O, V, C> mapMany(@Nonnull final Function<O, C> fn) {
		final MapManyAction<O, V, C> d = new MapManyAction<O, V, C>(fn, dispatcher);
		connect(d);
		return d;
	}

	/**
	 * {@link this#connect(Action)} all the passed {@param composables} to this {@link Stream},
	 * merging values streams into a new pipeline.
	 *
	 * @param composables the the composables to connect
	 * @return the merged stream
	 * @since 1.1, 2.0
	 */
	@SuppressWarnings("unchecked")
	@SafeVarargs
	public final MergeAction<O> merge(Publisher<O>... composables) {
		final List<Publisher<O>> publishers = new ArrayList<Publisher<O>>();

		publishers.add(this);
		Collections.addAll(publishers, composables);

		final MergeAction<O> mergeAction = new MergeAction<O>(dispatcher, null, null,
				publishers);

		mergeAction.capacity(batchSize).env(environment).setKeepAlive(keepAlive);
		return mergeAction;
	}

	/**
	 * {@link this#connect(Action)} all the flowing {@link Stream} values to a new {@link Stream}
	 *
	 * @return the merged stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <E> DynamicMergeAction<O, E, Stream<E>> merge() {
		final DynamicMergeAction<O, E, Stream<E>> mergeAction = new DynamicMergeAction<O, E, Stream<E>>(dispatcher);
		connect(mergeAction);
		return mergeAction;
	}

	/**
	 * Partition the stream output into N number of CPU cores sub-streams. Each partition will run on an exclusive
	 * {@link reactor.event.dispatch.RingBufferDispatcher}.
	 *
	 * @return A Stream of {@link Stream<O>}
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final Stream<Stream<O>> parallel() {
		return parallel(Runtime.getRuntime().availableProcessors());
	}

	/**
	 * Partition the stream output into N {@param poolsize} sub-streams. Each partition will run on an exclusive
	 * {@link reactor.event.dispatch.RingBufferDispatcher}.
	 *
	 * @param poolsize The level of concurrency to use
	 * @return A Stream of {@link Stream<O>}
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final Stream<Stream<O>> parallel(final Integer poolsize) {
		return parallel(poolsize, environment != null ?
				environment.getDefaulDispatcherFactory() :
				Environment.newDispatcherFactory(poolsize));
	}

	/**
	 * Partition the stream output into N {@param poolsize} sub-streams. EEach partition will run on an exclusive
	 * {@link Dispatcher} provided by the given {@param dispatcherSupplier}.
	 *
	 * @param poolsize           The level of concurrency to use
	 * @param dispatcherSupplier The {@link Supplier} to provide concurrent {@link Dispatcher}.
	 * @return A Stream of {@link Stream<O>}
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final Stream<Stream<O>> parallel(Integer poolsize, final Supplier<Dispatcher> dispatcherSupplier) {
		final ParallelAction<O> parallelAction = new ParallelAction<O>(
				this.dispatcher, dispatcherSupplier, poolsize
		);
		return connect(parallelAction);
	}

	/**
	 * Attach a No-Op Action that only serves the purpose of buffering incoming values if not enough demand is signaled
	 * downstream. A buffering capable stream will prevent underlying dispatcher to be saturated (and sometimes
	 * blocking).
	 *
	 * @return a buffered stream
	 * @since 2.0
	 */
	public Action<O, O> overflow() {
		return overflow(null);
	}

	/**
	 * Attach a No-Op Action that only serves the purpose of buffering incoming values if not enough demand is signaled
	 * downstream. A buffering capable stream will prevent underlying dispatcher to be saturated (and sometimes
	 * blocking).
	 *
	 * @param queue A completable queue {@link reactor.function.Supplier} to provide support for overflow
	 * @return a buffered stream
	 * @since 2.0
	 */
	public Action<O, O> overflow(CompletableQueue<O> queue) {
		Action<O, O> stream = Action.<O>passthrough(dispatcher);
		stream.capacity(batchSize).env(environment);
		stream.setKeepAlive(keepAlive);
		checkAndSubscribe(stream, queue != null ?
				createSubscription(stream).wrap(queue) :
				createSubscription(stream));
		return stream;
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
	 * @since 1.1, 2.0
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
	 * @return a new {@code Stream} whose values are generated on each request signal
	 * @since 1.1, 2.0
	 */
	public <E> SupplierAction<O, E> propagate(final Supplier<E> supplier) {
		final SupplierAction<O, E> d = new SupplierAction<O, E>(dispatcher, supplier);
		return connect(d);
	}

	/**
	 * Create a new {@code Stream} whose only value will be the current instance of the {@link Stream}.
	 *
	 * @return a new {@code Stream} whose only value will be the materialized current {@link Stream}
	 * @since 2.0
	 */
	public NestAction<O, Stream<O>> nest() {
		return connect(new NestAction<O, Stream<O>>(dispatcher, this));
	}

	/**
	 * Create a new {@code Stream} whose will re-subscribe its oldest parent-child stream pair. The action will start
	 * propagating errors after {@literal Integer.MAX_VALUE}.
	 *
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public RetryAction<O> retry() {
		return retry(Integer.MAX_VALUE);
	}

	/**
	 * Create a new {@code Stream} whose will re-subscribe its oldest parent-child stream pair. The action will start
	 * propagating errors after {@param numRetries}.
	 * This is generally useful for retry strategies and fault-tolerant streams.
	 *
	 * @param numRetries the number of times to tolerate an error
	 *
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public RetryAction<O> retry(int numRetries) {
		return retry(numRetries, null);
	}

	/**
	 * Create a new {@code Stream} whose will re-subscribe its oldest parent-child stream pair.
	 * {@param retryMatcher} will test an incoming {@link Throwable}, if positive the retry will occur.
	 * This is generally useful for retry strategies and fault-tolerant streams.
	 *
	 * @param retryMatcher the predicate to evaluate if retry should occur based on a given error signal
	 *
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public RetryAction<O> retry(Predicate<Throwable> retryMatcher) {
		return retry(Integer.MAX_VALUE, retryMatcher);
	}

	/**
	 * Create a new {@code Stream} whose will re-subscribe its oldest parent-child stream pair. The action will start
	 * propagating errors after {@param numRetries}. {@param retryMatcher} will test an incoming {@Throwable}, if positive
	 * the retry will occur (in conjonction with the {@param numRetries} condition).
	 * This is generally useful for retry strategies and fault-tolerant streams.
	 *
	 * @param numRetries the number of times to tolerate an error
	 * @param retryMatcher the predicate to evaluate if retry should occur based on a given error signal
	 *
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public RetryAction<O> retry(int numRetries, Predicate<Throwable> retryMatcher) {
		return connect(new RetryAction<O>(dispatcher, numRetries, retryMatcher));
	}

	/**
	 * Create a new {@code Stream} that will signal next elements up to {@param max} times.
	 *
	 * @param max the number of times to broadcast next signals before dropping
	 *
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public LimitAction<O> limit(long max) {
		return limit(max, null);
	}

	/**
	 * Create a new {@code Stream} that will signal next elements until {@param limitMatcher} is true.
	 *
	 * @param limitMatcher the predicate to evaluate for starting dropping events
	 *
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public LimitAction<O> limit(Predicate<O> limitMatcher) {
		return limit(Long.MAX_VALUE, limitMatcher);
	}

	/**
	 * Create a new {@code Stream} that will signal next elements until {@param limitMatcher} is true or
	 * up to {@param max} times.
	 *
	 * @param max the number of times to broadcast next signals before dropping
	 * @param limitMatcher the predicate to evaluate for starting dropping events
	 *
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public LimitAction<O> limit(long max, Predicate<O> limitMatcher) {
		return connect(new LimitAction<O>(dispatcher, limitMatcher, max));
	}

	/**
	 * Create a new {@code Stream} that accepts a {@link reactor.tuple.Tuple2} of T1 {@link Long} nanotime and T2 {@link <T>}
	 * associated data
	 *
	 * @return a new {@code Stream} that emits tuples of nano time and matching data
	 * @since 2.0
	 */
	public TimestampAction<O> timestamp() {
		return connect(new TimestampAction<O>(dispatcher));
	}

	/**
	 * Create a new {@code Stream} that accepts a {@link reactor.tuple.Tuple2} of T1 {@link Long} nanotime and T2 {@link <T>}
	 * associated data. The nanotime corresponds to the elapsed time between the subscribe and the first next signals OR
	 * between two next signals.
	 *
	 * @return a new {@code Stream} that emits tuples of nano time and matching data
	 * @since 2.0
	 */
	public ElapsedAction<O> elapsed() {
		return connect(new ElapsedAction<O>(dispatcher));
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch. Requires a {@code capacity}
	 * to
	 * have been set.
	 * 
	 * When a new batch is triggered, the first value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @return a new {@code Stream} whose values are the first value of each batch
	 */
	public FirstAction<O> first() {
		return first(batchSize);
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch. Requires a {@code capacity}
	 * to
	 * have been set.
	 * 
	 * When a new batch is triggered, the first value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @param batchSize the batch size to use
	 * @return a new {@code Stream} whose values are the first value of each batch)
	 */
	public FirstAction<O> first(int batchSize) {
		final FirstAction<O> d = new FirstAction<O>(batchSize, dispatcher);
		d.env(environment).setKeepAlive(keepAlive);
		subscribe(d);
		return d;
	}

	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code capacity}
	 *
	 * @return a new {@code Stream} whose values are the last value of each batch
	 */
	public LastAction<O> last() {
		return last(batchSize);
	}

	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code capacity}
	 *
	 * @param batchSize the batch size to use
	 * @return a new {@code Stream} whose values are the last value of each batch
	 */
	public LastAction<O> last(int batchSize) {
		final LastAction<O> d = new LastAction<O>(batchSize, dispatcher);
		d.env(environment).setKeepAlive(keepAlive);
		subscribe(d);
		return d;
	}


	/**
	 * Create a new {@code Stream} that filters out consecutive equals values.
	 *
	 * @return a new {@code Stream} whose values are the last value of each batch
	 *
	 * @since 2.0
	 */
	public Action<O, O> distinctUntilChanged() {
		final DistinctUntilChangedAction<O> d = new DistinctUntilChangedAction<O>(dispatcher);
		return connect(d);
	}


	/**
	 * Create a new {@code Stream} whose values will be each element E of any Iterable<E> flowing this Stream
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @return a new {@code Stream} whose values result from the iterable input
	 *
	 * @since 1.1, 2.0
	 */
	public <V> ForEachAction<V> split() {
		return split(Integer.MAX_VALUE);
	}

	/**
	 * Create a new {@code Stream} whose values will be each element E of any Iterable<E> flowing this Stream
	 * 
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @param batchSize the batch size to use
	 *
	 * @return a new {@code Stream} whose values result from the iterable input
	 *
	 * @since 1.1, 2.0
	 */
	@SuppressWarnings("unchecked")
	public <V> ForEachAction<V> split(int batchSize) {
		final ForEachAction<V> d = new ForEachAction<V>(dispatcher);
		final Stream<Iterable<V>> iterableStream = (Stream<Iterable<V>>) this;
		d.capacity(batchSize).env(environment).setKeepAlive(keepAlive);
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
	 * capacity} or flush is triggered has been reached.
	 *
	 * @return a new {@code Stream} whose values are a {@link java.util.List} of all values in this batch
	 */
	public BufferAction<O> buffer() {
		return buffer(batchSize);
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
	 * capacity} has been reached.
	 *
	 * @param batchSize the collected size
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this batch
	 */
	public BufferAction<O> buffer(int batchSize) {
		final BufferAction<O> d = new BufferAction<O>(batchSize, dispatcher);
		d.env(environment).setKeepAlive(keepAlive);
		subscribe(d);
		return d;
	}

	/**
	 * Collect incoming values into an internal array, providing a {@link List} that will be pushed into the returned
	 * {@code Stream}. The buffer will retain up to the last {@param backlog} elements in memory.
	 *
	 * @param backlog  maximum amount of items to keep
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this buffer
	 *
	 * @since 2.0
	 */
	public Stream<List<O>> movingBuffer(int backlog) {
		final Action<O, List<O>> d = new MovingBufferAction<O>(dispatcher, backlog);
		d.capacity(backlog).env(environment).setKeepAlive(keepAlive);
		subscribe(d);
		return d;
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the returned
	 * fresh {@link Stream}. Possible flush triggers are: {@link this#getMaxCapacity()}, complete signal or request signal.
	 * PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 *
	 * @return a new {@code Stream} whose values re-ordered using a PriorityQueue.
	 *
	 * @since 2.0
	 */
	public SortAction<O> sort() {
		return sort(null);
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the returned
	 * fresh {@link Stream}. Possible flush triggers are: {@param maxCapacity}, complete signal or request signal.
	 * PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 *
	 * @param maxCapacity a fixed maximum number or elements to re-order at once.
	 *
	 * @return a new {@code Stream} whose values re-ordered using a PriorityQueue.
	 *
	 * @since 2.0
	 */
	public SortAction<O> sort(int maxCapacity) {
		return sort(maxCapacity, null);
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the returned
	 * fresh {@link Stream}. Possible flush triggers are: {@link this#getMaxCapacity()}, complete signal or request signal.
	 * PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 *
	 * @param comparator A {@link Comparator<O>} to evaluate incoming data
	 *
	 * @return a new {@code Stream} whose values re-ordered using a PriorityQueue.
	 *
	 * @since 2.0
	 */
	public SortAction<O> sort(Comparator<O> comparator) {
		return sort(batchSize, comparator);
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the returned
	 * fresh {@link Stream}. Possible flush triggers are: {@param maxCapacity}, complete signal or request signal.
	 * PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 *
	 * @param maxCapacity a fixed maximum number or elements to re-order at once.
	 * @param comparator A {@link Comparator<O>} to evaluate incoming data
	 *
	 * @return a new {@code Stream} whose values re-ordered using a PriorityQueue.
	 *
	 * @since 2.0
	 */
	public SortAction<O> sort(int maxCapacity, Comparator<O> comparator) {
		final SortAction<O> d = new SortAction<O>(maxCapacity, dispatcher, comparator);
		d.env(environment).setKeepAlive(keepAlive);
		subscribe(d);
		return d;
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined {@link this#getMaxCapacity()}
	 * times. The nested streams will be pushed into the returned {@code Stream}.
	 *
	 * @return a new {@code Stream} whose values are a {@link Stream} of all values in this window
	 *
	 * @since 2.0
	 */
	public Stream<Stream<O>> window() {
		return window(batchSize);
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined {@param backlog} times.
	 * The nested streams will be pushed into the returned {@code Stream}.
	 *
	 * @param backlog the time period when each window close and flush the attached consumer
	 *
	 * @return a new {@code Stream} whose values are a {@link Stream} of all values in this window
	 *
	 * @since 2.0
	 */
	public Stream<Stream<O>> window(int backlog) {
		return connect(new WindowAction<O>(dispatcher, backlog));
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} for each unique key evaluated by the
	 * {param keyMapper}.
	 *
	 * @param keyMapper the key mapping function that evaluates an incoming data and returns a key.
	 *
	 * @return a new {@code Stream} whose values are a {@link Stream} of all values in this window
	 *
	 * @since 2.0
	 */
	public <K> Stream<GroupedByStream<K, O>> groupBy(Function<O,K> keyMapper) {
		return connect(new GroupByAction<O, K>(keyMapper, dispatcher));
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}. The given initial object will be
	 * passed to the function's {@link reactor.tuple.Tuple2} argument.
	 *
	 * @param fn      the reduce function
	 * @param initial the initial argument to pass to the reduce function
	 * @param <A>     the type of the reduced object
	 *
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Action<O, A> reduce(@Nonnull Function<Tuple2<O, A>, A> fn, A initial) {
		return reduce(fn, Functions.supplier(initial), batchSize);
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}. The given {@link Supplier} will be
	 * used to produce initial accumulator objects either on the first reduce call, in the case of an unbounded {@code
	 * Stream}, or on the first value of each batch, if a {@code capacity} is set.
	 * 
	 * In an unbounded {@code Stream}, the accumulated value will be published on the returned {@code Stream} on flush
	 * only. But when a {@code capacity} has been, the accumulated
	 * value will only be published on the new {@code Stream} at the end of each batch. On the next value (the first of
	 * the next batch), the {@link Supplier} is called again for a new accumulator object and the reduce starts over with
	 * a new accumulator.
	 *
	 * @param fn           the reduce function
	 * @param accumulators the {@link Supplier} that will provide accumulators
	 * @param batchSize    the batch size to use
	 * @param <A>          the type of the reduced object
	 *
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
	 *
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
	 *
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 *
	 * @since 1.1, 2.0
	 */
	public <A> Action<O, A> scan(@Nonnull Function<Tuple2<O, A>, A> fn, A initial) {
		return scan(fn, Functions.supplier(initial));
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}. The given {@link Supplier} will be
	 * used to produce initial accumulator objects either on the first reduce call, in the case of an unbounded {@code
	 * Stream}, or on the first value of each batch, if a {@code capacity} is set.
	 * 
	 * The accumulated value will be published on the returned {@code Stream} every time
	 * a
	 * value is accepted.
	 *
	 * @param fn           the scan function
	 * @param accumulators the {@link Supplier} that will provide accumulators
	 * @param <A>          the type of the reduced object
	 *
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 *
	 * @since 1.1, 2.0
	 */
	public <A> Action<O, A> scan(@Nonnull final Function<Tuple2<O, A>, A> fn, @Nullable final Supplier<A> accumulators) {
		return connect(new ScanAction<O, A>(accumulators,
				fn,
				dispatcher));
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}.
	 *
	 * @param fn  the reduce function
	 * @param <A> the type of the reduced object
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 * @since 1.1, 2.0
	 */
	public <A> Action<O, A> scan(@Nonnull final Function<Tuple2<O, A>, A> fn) {
		return scan(fn, (Supplier<A>) null);
	}

	/**
	 * Count accepted events for each batch and pass each accumulated long to the {@param stream}.
	 */
	public CountAction<O> count() {
		return count(batchSize);
	}

	/**
	 * Count accepted events for each batch {@param i} and pass each accumulated long to the {@param stream}.
	 */
	public CountAction<O> count(int i) {
		return connect(new CountAction<O>(dispatcher, i));
	}

	/**
	 * Request the parent stream when the last notification occurred after {@param
	 * timeout} milliseconds. Timeout is run on the environment root timer.
	 *
	 * @param timeout the timeout in milliseconds between two notifications on this composable
	 *
	 * @return this {@link Stream}
	 *
	 * @since 1.1
	 */
	public Action<O, O> timeout(long timeout) {
		Assert.state(getEnvironment() != null, "Cannot use default timer as no environment has been provided to this " +
				"Stream");
		return timeout(timeout, getEnvironment().getRootTimer());
	}

	/**
	 * Request the parent stream when the last notification occurred after {@param
	 * timeout} milliseconds. Timeout is run on the environment root timer.
	 *
	 * @param timeout the timeout in milliseconds between two notifications on this composable
	 * @param timer   the reactor timer to run the timeout on
	 *
	 * @return this {@link Stream}
	 *
	 * @since 1.1
	 */
	@SuppressWarnings("unchecked")
	public Action<O, O> timeout(long timeout, Timer timer) {
		final TimeoutAction<O> d = new TimeoutAction<O>(
				getDispatcher(),
				timer,
				timeout
		);
		return connect(d);
	}

	/**
	 * Request the parent stream every {@param period} milliseconds. Timeout is run on the environment root timer.
	 *
	 * @param period the period in milliseconds between two notifications on this stream
	 *
	 * @return this {@link Stream}
	 *
	 * @since 2.0
	 */
	public Action<O, O> throttle(long period) {
		Assert.state(getEnvironment() != null, "Cannot use default timer as no environment has been provided to this " +
				"Stream");
		return throttle(period, 0l);
	}

	/**
	 * Request the parent stream every {@param period} milliseconds after an initial {@param delay}.
	 * Timeout is run on the environment root timer.
	 *
	 * @param delay the timeout in milliseconds before starting consuming
	 * @param period the period in milliseconds between two notifications on this stream
	 *
	 * @return this {@link Stream}
	 *
	 * @since 2.0
	 */
	public Action<O, O> throttle(long period, long delay) {
		Assert.state(getEnvironment() != null, "Cannot use default timer as no environment has been provided to this " +
				"Stream");
		return throttle(period, delay, getEnvironment().getRootTimer());
	}

	/**
	 * Request the parent stream every {@param period} milliseconds after an initial {@param delay}.
	 * Timeout is run on the given {@param timer}.
	 *
	 * @param period the timeout in milliseconds between two notifications on this stream
	 * @param delay the timeout in milliseconds before starting consuming
	 * @param timer   the reactor timer to run the timeout on
	 *
	 * @return this {@link Stream}
	 *
	 * @since 2.0
	 */
	public Action<O, O> throttle(long period, long delay, Timer timer) {
		final ThrottleAction<O> d = new ThrottleAction<O>(
				getDispatcher(),
				timer,
				period,
				delay
		);
		d.env(environment).capacity(1).setKeepAlive(keepAlive);
		subscribe(d);

		return d;
	}

	/**
	 * Print a debugged form of the root composable relative to this. The output will be an acyclic directed graph of
	 * composed actions.
	 *
	 * @since 1.1, 2.0
	 */
	@SuppressWarnings("unchecked")
	public StreamUtils.StreamVisitor debug() {
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
		downstreamSubscription = null;
		state = State.READY;
		environment = null;
		keepAlive = false;
		error = null;
	}

	public <A, S extends Stream<A>> void subscribe(final CombineAction<O, A, S> subscriber) {
		subscribe(subscriber.input());
	}

	@Override
	public void subscribe(final Subscriber<O> subscriber) {
		checkAndSubscribe(subscriber, createSubscription(subscriber));
	}

	protected void checkAndSubscribe(Subscriber<O> subscriber, StreamSubscription<O> subscription) {
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
		if (!checkState() || downstreamSubscription == null) {
			return;
		}

		try {
			downstreamSubscription.onNext(ev);

			if (state == State.COMPLETE || downstreamSubscription.isComplete()) {
				downstreamSubscription.onComplete();
			}
		} catch (Throwable throwable) {
			callError(downstreamSubscription, throwable);
		}
	}

	@Override
	public void broadcastError(final Throwable throwable) {
		if (!checkState()) return;

		state = State.ERROR;
		error = throwable;

		if (downstreamSubscription == null) {
			log.error(this.getClass().getSimpleName() + " > broadcastError:" + this, new Exception(debug().toString(), throwable));
			return;
		}

		downstreamSubscription.onError(throwable);
	}

	@Override
	public void broadcastComplete() {
		if (!checkState()) return;

		if (downstreamSubscription == null) {
			state = State.COMPLETE;
			return;
		}

		try {
			downstreamSubscription.onComplete();
		} catch (Throwable throwable) {
			callError(downstreamSubscription, throwable);
		}

		state = State.COMPLETE;
	}

	public StreamSubscription<O> downstreamSubscription() {
		return downstreamSubscription;
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
	 * Return defined {@link Stream} capacity, used to drive new {@link org.reactivestreams.Subscription}
	 * request needs.
	 *
	 * @return
	 */
	public int getMaxCapacity() {
		return batchSize;
	}

	@SuppressWarnings("unchecked")
	protected boolean addSubscription(final StreamSubscription<O> subscription) {
		if (this.downstreamSubscription == null) {
			this.downstreamSubscription = subscription;
			return true;
		} else if (FanOutSubscription.class.isAssignableFrom(this.downstreamSubscription.getClass())) {
			return ((FanOutSubscription<O>) this.downstreamSubscription).getSubscriptions().add(subscription);
		} else {
			this.downstreamSubscription = new FanOutSubscription<O>(this, this.downstreamSubscription, subscription);
			return true;
		}
	}

	protected void removeSubscription(final StreamSubscription<O> subscription) {
		if (this.downstreamSubscription == null) return;

		if (subscription == this.downstreamSubscription) {
			this.downstreamSubscription = null;
			if (!keepAlive) {
				state = State.SHUTDOWN;
			}
		} else if (FanOutSubscription.class.isAssignableFrom(this.downstreamSubscription.getClass())) {
			((FanOutSubscription<O>) this.downstreamSubscription).getSubscriptions().remove(subscription);

			if (((FanOutSubscription<O>) subscription).getSubscriptions().isEmpty() && !keepAlive) {
				state = State.SHUTDOWN;
			}
		}

	}

	protected void setState(State state) {
		this.state = state;
	}

	private void callError(StreamSubscription<O> subscription, Throwable cause) {
		subscription.onError(cause);
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

	public void dispatch(Consumer<Void> action) {
		dispatch(null, action);
	}

	public <E> void dispatch(E data, Consumer<E> action) {
		dispatcher.dispatch(this, data, null, null, ROUTER, action);
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
