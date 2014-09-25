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
import reactor.event.lifecycle.Pausable;
import reactor.event.routing.ArgumentConvertingConsumerInvoker;
import reactor.event.routing.ConsumerFilteringRouter;
import reactor.event.routing.Router;
import reactor.event.selector.Selectors;
import reactor.filter.PassThroughFilter;
import reactor.function.*;
import reactor.function.support.Tap;
import reactor.queue.CompletableBlockingQueue;
import reactor.queue.CompletableQueue;
import reactor.rx.action.*;
import reactor.rx.action.support.SpecificationExceptions;
import reactor.timer.Timer;
import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;
import reactor.tuple.TupleN;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Base class for components designed to provide a succinct API for working with future values.
 * Provides base functionality and an internal contract for subclasses that make use of
 * the {@link #map(reactor.function.Function)} and {@link #filter(reactor.function.Predicate)} methods.
 * <p>
 * A Stream can be implemented to perform specific actions on callbacks (doNext,doComplete,doError,doSubscribe).
 * It is an asynchronous boundary and will run the callbacks using the input {@link Dispatcher}. Stream can
 * eventually produce a result {@param <O>} and will offer cascading over its own subscribers.
 * <p>
 * *
 * Typically, new {@code Stream Streams} aren't created directly. To create a {@code Stream},
 * create a {@link reactor.rx.spec.Streams} and configure it with the appropriate {@link Environment},
 * {@link Dispatcher}, and other settings.
 *
 * @param <O> The type of the output values
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1, 2.0
 */
public class Stream<O> implements Pausable, Publisher<O>, Recyclable {

	protected static final Logger log = LoggerFactory.getLogger(Stream.class);

	public static final Router ROUTER = new ConsumerFilteringRouter(
			new PassThroughFilter(), new ArgumentConvertingConsumerInvoker(null)
	);

	private StreamSubscription<O> downstreamSubscription;

	protected final Dispatcher dispatcher;

	protected Throwable error = null;
	protected boolean   pause = false;
	protected long capacity;

	protected boolean keepAlive    = true;
	protected boolean ignoreErrors = false;
	protected Environment environment;
	protected State state = State.READY;

	public Stream() {
		this(Long.MAX_VALUE);

	}

	public Stream(long capacity) {
		this.dispatcher = SynchronousDispatcher.INSTANCE;
		this.capacity = capacity;
	}

	public Stream(Dispatcher dispatcher) {
		this(dispatcher, dispatcher.backlogSize() - Action.RESERVED_SLOTS);
	}

	public Stream(Dispatcher dispatcher, long capacity) {
		this(dispatcher, null, capacity);
	}

	public Stream(@Nonnull Dispatcher dispatcher,
	              @Nullable Environment environment,
	              long capacity) {
		this.environment = environment;
		this.dispatcher = dispatcher;
		this.capacity = capacity;
	}

	/**
	 * Bind the stream to a given {@param elements} volume of in-flight data:
	 * - An {@link Action} will request up to the defined volume upstream.
	 * - An {@link Action} will track the pending requests and fire up to {@param elements} when the previous volume has
	 * been processed.
	 * - A {@link BatchAction} and any other size-bound action will be limited to the defined volume.
	 * <p>
	 * <p>
	 * A stream capacity can't be superior to the underlying dispatcher capacity: if the {@param elements} overflow the
	 * dispatcher backlog size, the capacity will be aligned automatically to fit it. A warning message should signal
	 * such behavior.
	 * RingBufferDispatcher will for instance limit to a power of 2 size up to {@literal Integer.MAX_VALUE},
	 * where a Stream can be sized up to {@literal Long.MAX_VALUE} in flight data.
	 * <p>
	 * <p>
	 * When the stream receives more elements than requested, incoming data is eventually staged in the eventual {@link
	 * org.reactivestreams.Subscription}.
	 * The subscription can react differently according to the implementation in-use,
	 * the default strategy is as following:
	 * - The first-level of pair compositions Stream->Action will overflow data in a {@link CompletableQueue},
	 * ready to be polled when the action fire the pending requests.
	 * - The following pairs of Action->Action will synchronously pass data
	 * - Any pair of Stream->Subscriber or Action->Subscriber will behave as with the root Stream->Action pair rule.
	 * - {@link this#onOverflowBuffer()} force this staging behavior, with a possibilty to pass a {@link reactor.queue
	 * .PersistentQueue}
	 *
	 * @param elements
	 * @return {@literal this}
	 */
	public Stream<O> capacity(long elements) {
		this.capacity = elements > (dispatcher.backlogSize() - Action.RESERVED_SLOTS) ?
				dispatcher.backlogSize() - Action.RESERVED_SLOTS : elements;
		if (log.isTraceEnabled() && capacity != elements) {
			log.trace(" The assigned capacity is now {}. The Stream altered the requested maximum capacity {} to not " +
							"overrun" +
							" " +
							"its Dispatcher which supports " +
							"up to {} slots for next signals, minus {} slots error|" +
							"complete|subscribe|request.",
					capacity, elements, dispatcher.backlogSize(), Action.RESERVED_SLOTS);
		}
		return this;
	}

	/**
	 * Update the environment used by this {@link Stream}
	 *
	 * @param environment
	 * @return {@literal this}
	 */
	public Stream<O> env(Environment environment) {
		this.environment = environment;
		return this;
	}

	/**
	 * Update the keep-alive property used by this {@link Stream}. When kept-alive, the stream will not shutdown
	 * automatically on complete. Shutdown state is observed when the last subscriber is cancelled.
	 *
	 * @param keepAlive
	 * @return {@literal this}
	 */
	public Stream<O> keepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
		return this;
	}

	/**
	 * Update the ignore-errors property used by this {@link Stream}. When ignoring, the stream will not terminate on
	 * error
	 *
	 * @param ignore
	 * @return {@literal this}
	 */
	public Stream<O> ignoreErrors(boolean ignore) {
		this.ignoreErrors = ignore;
		return this;
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

	/**
	 * Create a new {@code Stream} whose values will be the current instance of the {@link Stream}. Everytime
	 * the {@param controlStream} receives a next signal, the current Stream and the input data will be published as a
	 * {@link reactor.tuple.Tuple2} to the attached {@param controller}.
	 * <p>
	 * This is particulary useful to dynamically adapt the {@link Stream} instance : capacity, pause(), resume()...
	 *
	 * @param controlStream The consumed stream, each signal will trigger the passed controller
	 * @param controller    The consumer accepting a pair of Stream and user-provided signal type
	 * @return the current {@link Stream} instance
	 * @since 2.0
	 */
	public <E> Stream<O> control(Stream<E> controlStream, final Consumer<Tuple2<Stream<O>, ? super E>> controller) {
		final Stream<O> thiz = this;
		controlStream.consume(new Consumer<E>() {
			@Override
			public void accept(E e) {
				controller.accept(Tuple.of(thiz, e));
			}
		});
		return this;
	}

	/**
	 * Cast the current Stream flowing data type into a target class type.
	 *
	 * @param <E> the {@link Action} output type
	 * @return the current {link Stream} instance casted
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public <E> Stream<E> cast(@Nonnull final Class<E> stream) {
		return (Stream<E>) this;
	}

	/**
	 * Subscribe an {@link Action} to the actual pipeline.
	 * Additionally to producing events (error,complete,next and eventually flush), it will take care of setting the
	 * environment if available.
	 * It will also give an initial capacity size used for {@link org.reactivestreams.Subscription#request(long)} ONLY
	 * IF the passed action capacity is not the default Long.MAX_VALUE ({@see this#capacity(elements)}.
	 * Current KeepAlive value is also assigned
	 * <p>
	 * Reactive Extensions patterns also dubs this operation "lift".
	 * The operation is returned for functional-style chaining.
	 * <p>
	 * If the new action dispatcher is different, the new action will take
	 * care of buffering incoming data into a StreamSubscription. Otherwise default behavior is picked:
	 * FireHose synchronous subscription is the parent stream != null
	 *
	 * @param action the processor to subscribe.
	 * @param <E>    the {@link Action} output type
	 * @return the current {link Stream} instance
	 * @see {@link org.reactivestreams.Publisher#subscribe(org.reactivestreams.Subscriber)}
	 * @since 2.0
	 */
	public final <A, E extends Action<? super O, ? extends A>> E connect(@Nonnull final E action) {
		if ((action.capacity == Long.MAX_VALUE && capacity != Long.MAX_VALUE) ||
				action.capacity == action.dispatcher.backlogSize() - Action.RESERVED_SLOTS) {
			action.capacity(capacity);
		}

		action.env(environment).keepAlive(keepAlive);

		if (action.dispatcher != this.dispatcher) {
			this.checkAndSubscribe(action, createSubscription(action, true));
		} else {
			this.subscribe(action);
		}
		return action;
	}

	/**
	 * Subscribe an {@link Subscriber} to the actual pipeline. Additionally to producing events (error,complete,next,
	 * subscribe),
	 * Reactive Extensions patterns also dubs this operation "lift".
	 *
	 * @param stream the processor to subscribe.
	 * @param <E>    the {@link Subscriber} output type
	 * @return the current {link Stream} instance
	 * @see {@link org.reactivestreams.Publisher#subscribe(org.reactivestreams.Subscriber)}
	 * @since 2.0
	 */
	public final <E extends Subscriber<? super O>> E connect(@Nonnull final E stream) {
		this.subscribe(stream);
		return stream;
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
	public final <E extends Throwable> Action<O, O> when(@Nonnull final Class<E> exceptionType,
	                                                     @Nonnull final Consumer<E> onError) {
		return connect(new ErrorAction<O, E>(dispatcher, Selectors.T(exceptionType), onError));
	}

	/**
	 * Materialize an error state into a downstream event.
	 *
	 * @param exceptionType the type of exceptions to handle
	 * @param <E>           type of the exception to handle
	 * @return {@literal this}
	 * @since 2.0
	 */
	public final <E extends Throwable> Action<O, E> recover(@Nonnull final Class<E> exceptionType) {
		RecoverAction<O, E> recoverAction = new RecoverAction<O, E>(dispatcher, Selectors.T(exceptionType));
		return connect(recoverAction);
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will consume any values accepted by this {@code
	 * Stream}. As such this a terminal action to be placed on a stream flow. Only error and complete signal will be
	 * signaled downstream. It will also eagerly prefetch upstream publisher.
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link this#observe(reactor.function.Consumer)}
	 *
	 * @param consumer the consumer to invoke on each value
	 * @return {@literal this}
	 */
	public final Action<O, Void> consume(@Nonnull final Consumer<? super O> consumer) {
		return connect(new TerminalCallbackAction<O>(dispatcher, consumer, null, null));
	}

	/**
	 * Attach 2 {@link Consumer} to this {@code Stream} that will consume any values signaled by this {@code
	 * Stream}. As such this a terminal action to be placed on a stream flow.
	 * Any Error signal will be consumed by the error consumer.
	 * Only error and complete signal will be
	 * signaled downstream. It will also eagerly prefetch upstream publisher.
	 * <p>
	 *
	 * @param consumer the consumer to invoke on each next signal
	 * @param consumer the consumer to invoke on each error signal
	 * @return {@literal this}
	 */
	public final Action<O, Void> consume(@Nonnull final Consumer<? super O> consumer,
	                                     Consumer<? super Throwable> errorConsumer) {
		return connect(new TerminalCallbackAction<O>(dispatcher, consumer, errorConsumer, null));
	}

	/**
	 * Attach 3 {@link Consumer} to this {@code Stream} that will consume any values signaled by this {@code
	 * Stream}. As such this a terminal action to be placed on a stream flow.
	 * Any Error signal will be consumed by the error consumer.
	 * The Complete signal will be consumed by the complete consumer.
	 * Only error and complete signal will be signaled downstream. It will also eagerly prefetch upstream publisher.
	 * <p>
	 *
	 * @param consumer the consumer to invoke on each value
	 * @return {@literal this}
	 */
	public final Action<O, Void> consume(@Nonnull final Consumer<? super O> consumer,
	                                     Consumer<? super Throwable> errorConsumer,
	                                     Consumer<Void> completeConsumer) {
		return connect(new TerminalCallbackAction<O>(dispatcher, consumer, errorConsumer, completeConsumer));
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will observe any values accepted by this {@code
	 * Stream}.
	 *
	 * @param consumer the consumer to invoke on each value
	 * @return {@literal this}
	 * @since 2.0
	 */
	public final Action<O, O> observe(@Nonnull final Consumer<? super O> consumer) {
		return connect(new CallbackAction<O>(dispatcher, consumer));
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will observe terminal signal complete|error. It will pass
	 * the
	 * newly created {@link Stream} to the consumer for state introspection, e.g. {@link #getState()}
	 * Stream}.
	 *
	 * @param consumer the consumer to invoke on terminal signal
	 * @return {@literal this}
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <E> Action<O, O> finallyDo(Consumer<E> consumer) {
		return connect(new FinallyAction<O, E>(dispatcher, (E) this, consumer));
	}

	/**
	 * Pass values accepted by this {@code Stream} into the given {@link Observable}, notifying with the given key.
	 *
	 * @param key        the key to notify on
	 * @param observable the {@link Observable} to notify
	 * @return {@literal this}
	 * @since 1.1, 2.0
	 */
	public final Action<O, Void> notify(@Nonnull final Object key, @Nonnull final Observable observable) {
		return connect(new ObservableAction<O>(dispatcher, observable, key));
	}

	/**
	 * Assign the a new Dispatcher to the returned Stream. If the dispatcher is different, the new action will take
	 * care of buffering incoming data into a StreamSubscription. Otherwise default behavior is picked:
	 * FireHose synchronous subscription is the parent stream != null
	 *
	 * @param dispatcher the new dispatcher
	 * @return a new {@link Action} running on a different {@link Dispatcher}
	 */
	public final Action<O, O> dispatchOn(@Nonnull final Dispatcher dispatcher) {
		Assert.state(dispatcher.supportsOrdering(), "Dispatcher provided doesn't support event ordering. " +
				" Refer to #parallel() method. ");
		return connect(Action.<O>passthrough(dispatcher));
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code V} and pass it into
	 * another {@code Stream}.
	 *
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@link Action} containing the transformed values
	 */
	public final <V> Action<O, V> map(@Nonnull final Function<? super O, ? extends V> fn) {
		return connect(new MapAction<O, V>(fn, dispatcher));
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code Stream<O,V>} and pass
	 * it into another {@code Stream}.
	 *
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@link Action} containing the transformed values
	 * @since 1.1, 2.0
	 */
	public final <V> Action<Publisher<? extends V>, V> flatMap(@Nonnull final Function<? super O,
			? extends Publisher<? extends V>> fn) {
		return map(fn).merge();
	}

	/**
	 * {@link this#connect(Action)} all the nested {@link Publisher} values to a new {@link Stream}.
	 * Dynamic merge requires use of reactive-pull
	 * offered by default StreamSubscription. If merge hasn't capacity to take new elements because its {@link
	 * this#capacity(long)} instructed so, the subscription will buffer
	 * them.
	 *
	 * @param <V> the inner stream flowing data type that will be the produced signal.
	 * @return the merged stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <V> Action<Publisher<? extends V>, V> merge() {
		return fanIn(null);
	}

	/**
	 * {@link this#connect(Action)} all the nested {@link Publisher} values from this current upstream and from the
	 * passed publisher.
	 *
	 * @return the merged stream
	 * @since 2.0
	 */
	public final Action<O, O> mergeWith(Publisher<? extends O> publisher) {
		return new MergeAction<O>(dispatcher, Arrays.<Publisher<? extends O>>asList(this, publisher))
				.env(environment).keepAlive(keepAlive);
	}

	/**
	 * {@link this#connect(Action)} all the nested {@link Publisher} values to a new {@link Stream} until one of them
	 * complete.
	 * The result will be produced with a list of each upstream most recent emitted data.
	 *
	 * @return the zipped and joined stream
	 * @since 2.0
	 */
	public final <V> Action<Publisher<?>, List<V>> join() {
		return zip(ZipAction.<TupleN, V>joinZipper());
	}


	/**
	 * {@link this#connect(Action)} all the nested {@link Publisher} values to a new {@link Stream} until one of them
	 * complete.
	 * The result will be produced with a list of each upstream most recent emitted data.
	 *
	 * @return the zipped and joined stream
	 * @since 2.0
	 */
	public final <V> Action<?, List<V>> joinWith(Publisher<? extends V> publisher) {
		return zipWith(publisher, ZipAction.<Tuple2<O, V>, V>joinZipper());
	}

	/**
	 * {@link this#connect(Action)} all the nested {@link Publisher} values to a new {@link Stream} until one of them
	 * complete.
	 * The result will be produced by the zipper transformation from a tuple of each upstream most recent emitted data.
	 *
	 * @return the merged stream
	 * @since 2.0
	 */
	public final <V> Action<Publisher<?>, V> zip(@Nonnull Function<TupleN, ? extends V> zipper) {
		return fanIn(new ZipAction<Object, V, TupleN>(dispatcher, zipper, null));
	}

	/**
	 * {@link this#connect(Action)} with the passed {@link Publisher} values to a new {@link Stream} until one of them
	 * complete.
	 * The result will be produced by the zipper transformation from a tuple of each upstream most recent emitted data.
	 *
	 * @return the zipped stream
	 * @since 2.0
	 */
	public final <T2, V> Action<?, V> zipWith(Publisher<? extends T2> publisher,
	                                          @Nonnull Function<Tuple2<O, T2>, V> zipper) {
		return new ZipAction<>(dispatcher, zipper, Arrays.asList(this, publisher)).env(environment);
	}

	/**
	 * {@link this#connect(Action)} all the nested {@link Publisher} values to a new {@link Stream} until one of them
	 * complete.
	 * The result will be produced by the zipper transformation from a tuple of each upstream most recent emitted data.
	 *
	 * @return the zipped stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <T2, V> Action<?, V> zipWith(Iterable<? extends T2> iterable,
	                                          @Nonnull Function<Tuple2<O, T2>, V> zipper) {
		return zipWith(new ForEachAction<T2>(iterable, dispatcher).env(environment).keepAlive(keepAlive), zipper);
	}

	/**
	 * {@link this#connect(Action)} all the nested {@link Publisher} values to a new {@link Stream} calling the logic
	 * inside the provided fanInAction for complex merging strategies.
	 * {@link reactor.rx.action.FanInAction} provides helpers to create subscriber for each source,
	 * a registry of incoming sources and overriding doXXX signals as usual to produce the result via
	 * Stream#broadcastXXX.
	 * <p>
	 * A default fanInAction will act like {@link this#merge()}, passing values to doNext. In java8 one can then
	 * implement
	 * stream.fanIn(data -> broadcastNext(data)) or stream.fanIn(System.out::println)
	 * <p>
	 * <p>
	 * Dynamic merge (moving nested data into the top-level returned stream) requires use of reactive-pull offered by
	 * default StreamSubscription. If merge hasn't capacity to
	 * take new elements because its {@link
	 * this#capacity(long)} instructed so, the subscription will buffer
	 * them.
	 *
	 * @param <T> the nested type of flowing upstream Stream.
	 * @param <V> the produced output
	 * @return the zipped stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <T, V> Action<Publisher<? extends T>, V> fanIn(
			FanInAction<T, V, ? extends FanInAction.InnerSubscriber<T, V>> fanInAction
	) {
		Stream<Publisher<T>> thiz = (Stream<Publisher<T>>) this;

		Action<Publisher<? extends T>, V> innerMerge = new DynamicMergeAction<T, V>(dispatcher, fanInAction);
		innerMerge.capacity(capacity).env(environment);
		innerMerge.keepAlive(keepAlive);

		thiz.checkAndSubscribe(innerMerge, thiz.createSubscription(innerMerge, true));
		//thiz.connect(innerMerge);
		return innerMerge;
	}

	/**
	 * Partition the stream output into N number of CPU cores sub-streams. Each partition will run on an exclusive
	 * {@link reactor.event.dispatch.RingBufferDispatcher}.
	 *
	 * @return A Stream of {@link Stream<O>}
	 * @since 2.0
	 */
	public final ParallelAction<O> parallel() {
		return parallel(Environment.PROCESSORS);
	}

	/**
	 * Partition the stream output into N {@param poolsize} sub-streams. Each partition will run on an exclusive
	 * {@link reactor.event.dispatch.RingBufferDispatcher}.
	 *
	 * @param poolsize The level of concurrency to use
	 * @return A Stream of {@link Stream<O>}
	 * @since 2.0
	 */
	public final ParallelAction<O> parallel(final Integer poolsize) {
		return parallel(poolsize, environment != null ?
				environment.getDefaultDispatcherFactory() :
				Environment.newSingleProducerMultiConsumerDispatcherFactory(poolsize, "parallel-stream"));
	}

	/**
	 * Partition the stream output into N {@param poolsize} sub-streams. EEach partition will run on an exclusive
	 * {@link Dispatcher} provided by the given {@param dispatcherSupplier}.
	 *
	 * @param poolsize           The level of concurrency to use
	 * @param dispatcherSupplier The {@link Supplier} to provide concurrent {@link Dispatcher}.
	 * @return A Stream of {@link Action}
	 * @since 2.0
	 */
	public final ParallelAction<O> parallel(Integer poolsize, final Supplier<Dispatcher> dispatcherSupplier) {
		return connect(new ParallelAction<O>(
				this.dispatcher, dispatcherSupplier, poolsize
		));
	}

	/**
	 * Attach a No-Op Action that only serves the purpose of buffering incoming values if not enough demand is signaled
	 * downstream. A buffering capable stream will prevent underlying dispatcher to be saturated (and sometimes
	 * blocking).
	 *
	 * @return a buffered stream
	 * @since 2.0
	 */
	public final FlowControlAction<O> onOverflowBuffer() {
		return onOverflowBuffer(null);
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
	public final FlowControlAction<O> onOverflowBuffer(CompletableQueue<O> queue) {
		FlowControlAction<O> stream = new FlowControlAction<O>(dispatcher);
		if (queue != null) {
			stream.capacity(capacity).env(environment);
			stream.keepAlive(keepAlive);
			checkAndSubscribe(stream, createSubscription(stream, true).wrap(queue));
		} else {
			connect(stream);
		}
		return stream;
	}

	/**
	 * Evaluate each accepted value against the given {@link Predicate}. If the predicate test succeeds, the value is
	 * passed into the new {@code Stream}. If the predicate test fails, the value is propagated into the {@link
	 * reactor.rx.action.FilterAction#otherwise()} composable .
	 *
	 * @param p the {@link Predicate} to test values against
	 * @return a new {@link Action} containing only values that pass the predicate test
	 */
	public final FilterAction<O, Stream<O>> filter(final Predicate<? super O> p) {
		return connect(new FilterAction<O, Stream<O>>(p, dispatcher));
	}

	/**
	 * Evaluate each accepted boolean value. If the predicate test succeeds, the value is
	 * passed into the new {@code Stream}. If the predicate test fails, the value is ignored.
	 *
	 * @return a new {@link Action} containing only values that pass the predicate test
	 * @since 1.1, 2.0
	 */
	@SuppressWarnings("unchecked")
	public final FilterAction<Boolean, Stream<Boolean>> filter() {
		return ((Stream<Boolean>) this).filter(FilterAction.simplePredicate);
	}

	/**
	 * Create a new {@code Stream} whose only value will be the current instance of the {@link Stream}.
	 *
	 * @return a new {@link Action} whose only value will be the materialized current {@link Stream}
	 * @since 2.0
	 */
	public final Action<O, Stream<O>> nest() {
		return connect(new NestAction<O, Stream<O>, Object>(dispatcher, this));
	}

	/**
	 * Create a new {@code Stream} whose will re-subscribe its oldest parent-child stream pair. The action will start
	 * propagating errors after {@literal Integer.MAX_VALUE}.
	 *
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final RetryAction<O> retry() {
		return retry(Integer.MAX_VALUE);
	}

	/**
	 * Create a new {@code Stream} whose will re-subscribe its oldest parent-child stream pair. The action will start
	 * propagating errors after {@param numRetries}.
	 * This is generally useful for retry strategies and fault-tolerant streams.
	 *
	 * @param numRetries the number of times to tolerate an error
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final RetryAction<O> retry(int numRetries) {
		return retry(numRetries, null);
	}

	/**
	 * Create a new {@code Stream} whose will re-subscribe its oldest parent-child stream pair.
	 * {@param retryMatcher} will test an incoming {@link Throwable}, if positive the retry will occur.
	 * This is generally useful for retry strategies and fault-tolerant streams.
	 *
	 * @param retryMatcher the predicate to evaluate if retry should occur based on a given error signal
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final RetryAction<O> retry(Predicate<Throwable> retryMatcher) {
		return retry(Integer.MAX_VALUE, retryMatcher);
	}

	/**
	 * Create a new {@code Stream} whose will re-subscribe its oldest parent-child stream pair. The action will start
	 * propagating errors after {@param numRetries}. {@param retryMatcher} will test an incoming {@Throwable},
	 * if positive
	 * the retry will occur (in conjonction with the {@param numRetries} condition).
	 * This is generally useful for retry strategies and fault-tolerant streams.
	 *
	 * @param numRetries   the number of times to tolerate an error
	 * @param retryMatcher the predicate to evaluate if retry should occur based on a given error signal
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final RetryAction<O> retry(int numRetries, Predicate<Throwable> retryMatcher) {
		return connect(new RetryAction<O>(dispatcher, numRetries, retryMatcher));
	}

	/**
	 * Create a new {@code Stream} that will signal next elements up to {@param max} times.
	 *
	 * @param max the number of times to broadcast next signals before dropping
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Action<O, O> limit(long max) {
		return limit(max, null);
	}

	/**
	 * Create a new {@code Stream} that will signal next elements until {@param limitMatcher} is true.
	 *
	 * @param limitMatcher the predicate to evaluate for starting dropping events
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Action<O, O> limit(Predicate<O> limitMatcher) {
		return limit(Long.MAX_VALUE, limitMatcher);
	}

	/**
	 * Create a new {@code Stream} that will signal next elements until {@param limitMatcher} is true or
	 * up to {@param max} times.
	 *
	 * @param max          the number of times to broadcast next signals before dropping
	 * @param limitMatcher the predicate to evaluate for starting dropping events
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final LimitAction<O> limit(long max, Predicate<O> limitMatcher) {
		return connect(new LimitAction<O>(dispatcher, limitMatcher, max));
	}

	/**
	 * Create a new {@code Stream} that accepts a {@link reactor.tuple.Tuple2} of T1 {@link Long} nanotime and T2 {@link
	 * <T>}
	 * associated data
	 *
	 * @return a new {@link Action} that emits tuples of nano time and matching data
	 * @since 2.0
	 */
	public final Action<O, Tuple2<Long, O>> timestamp() {
		return connect(new TimestampAction<O>(dispatcher));
	}

	/**
	 * Create a new {@code Stream} that accepts a {@link reactor.tuple.Tuple2} of T1 {@link Long} nanotime and T2 {@link
	 * <T>}
	 * associated data. The nanotime corresponds to the elapsed time between the subscribe and the first next signals OR
	 * between two next signals.
	 *
	 * @return a new {@link Action} that emits tuples of nano time and matching data
	 * @since 2.0
	 */
	public final Action<O, Tuple2<Long, O>> elapsed() {
		return connect(new ElapsedAction<O>(dispatcher));
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch. Requires a {@code capacity}
	 * to
	 * have been set.
	 * <p>
	 * When a new batch is triggered, the first value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @return a new {@link Action} whose values are the first value of each batch
	 */
	public final Action<O, O> first() {
		return first(capacity);
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch. Requires a {@code capacity}
	 * to
	 * have been set.
	 * <p>
	 * When a new batch is triggered, the first value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @param batchSize the batch size to use
	 * @return a new {@link Action} whose values are the first value of each batch)
	 */
	public final Action<O, O> first(long batchSize) {
		return connect(new FirstAction<O>(batchSize, dispatcher));
	}


	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code capacity}
	 *
	 * @return a new {@link Action} whose values are the last value of each batch
	 */
	public final Action<O, O> last() {
		return every(capacity);
	}


	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code capacity}
	 *
	 * @param batchSize the batch size to use
	 * @return a new {@link Action} whose values are the last value of each batch
	 */
	public final Action<O, O> every(long batchSize) {
		return connect(new LastAction<O>(batchSize, dispatcher));
	}

	/**
	 * Create a new {@code Stream} that filters out consecutive equals values.
	 *
	 * @return a new {@link Action} whose values are the last value of each batch
	 * @since 2.0
	 */
	public final Action<O, O> distinctUntilChanged() {
		final DistinctUntilChangedAction<O> d = new DistinctUntilChangedAction<O>(dispatcher);
		return connect(d);
	}

	/**
	 * Create a new {@code Stream} whose values will be each element E of any Iterable<E> flowing this Stream
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @return a new {@link Action} whose values result from the iterable input
	 * @since 1.1, 2.0
	 */
	public final <V> Action<Iterable<? extends V>, V> split() {
		return split(Long.MAX_VALUE);
	}

	/**
	 * Create a new {@code Stream} whose values will be each element E of any Iterable<E> flowing this Stream
	 * <p>
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @param batchSize the batch size to use
	 * @return a new {@link Action} whose values result from the iterable input
	 * @since 1.1, 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <V> Action<Iterable<? extends V>, V> split(final long batchSize) {
		final Stream<Iterable<V>> iterableStream = (Stream<Iterable<V>>) this;
		return iterableStream.connect(new ForEachAction<V>(null, dispatcher).capacity(batchSize));
	}

	/**
	 * Create a {@link reactor.function.support.Tap} that maintains a reference to the last value seen by this {@code
	 * Stream}. The {@link reactor.function.support.Tap} is
	 * continually updated when new values pass through the {@code Stream}.
	 *
	 * @return the new {@link reactor.function.support.Tap}
	 * @see Consumer
	 */
	public final Tap<O> tap() {
		final Tap<O> tap = new Tap<O>();
		connect(new TerminalCallbackAction<O>(dispatcher, tap, null, null));
		return tap;
	}

	/**
	 * Collect incoming values into a {@link java.util.List} that will be pushed into the returned {@code Stream} every
	 * time {@code
	 * capacity} or flush is triggered has been reached.
	 *
	 * @return a new {@link Action} whose values are a {@link java.util.List} of all values in this batch
	 */
	public final Action<O, List<O>> buffer() {
		return buffer(capacity);
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
	 * capacity} has been reached.
	 *
	 * @param batchSize the collected size
	 * @return a new {@link Action} whose values are a {@link List} of all values in this batch
	 */
	public final Action<O, List<O>> buffer(long batchSize) {
		return connect(new BufferAction<O>(batchSize, dispatcher));
	}

	/**
	 * Collect incoming values into an internal array, providing a {@link List} that will be pushed into the returned
	 * {@code Stream}. The buffer will retain up to the last {@param backlog} elements in memory.
	 *
	 * @param backlog maximum amount of items to keep
	 * @return a new {@link Action} whose values are a {@link List} of all values in this buffer
	 * @since 2.0
	 */
	public final Action<O, List<O>> movingBuffer(int backlog) {
		return connect(new MovingBufferAction<O>(dispatcher, backlog, 1));
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the
	 * returned
	 * fresh {@link Stream}. Possible flush triggers are: {@link this#getCapacity()},
	 * complete signal or request signal.
	 * PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 *
	 * @return a new {@link Action} whose values re-ordered using a PriorityQueue.
	 * @since 2.0
	 */
	public final Action<O, O> sort() {
		return sort(null);
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the
	 * returned
	 * fresh {@link Stream}. Possible flush triggers are: {@param maxCapacity}, complete signal or request signal.
	 * PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 *
	 * @param maxCapacity a fixed maximum number or elements to re-order at once.
	 * @return a new {@link Action} whose values re-ordered using a PriorityQueue.
	 * @since 2.0
	 */
	public final Action<O, O> sort(int maxCapacity) {
		return sort(maxCapacity, null);
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the
	 * returned
	 * fresh {@link Stream}. Possible flush triggers are: {@link this#getCapacity()},
	 * complete signal or request signal.
	 * PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 *
	 * @param comparator A {@link Comparator<O>} to evaluate incoming data
	 * @return a new {@link Action} whose values re-ordered using a PriorityQueue.
	 * @since 2.0
	 */
	public final Action<O, O> sort(Comparator<? super O> comparator) {
		return sort((int) capacity, comparator);
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the
	 * returned
	 * fresh {@link Stream}. Possible flush triggers are: {@param maxCapacity}, complete signal or request signal.
	 * PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 *
	 * @param maxCapacity a fixed maximum number or elements to re-order at once.
	 * @param comparator  A {@link Comparator<O>} to evaluate incoming data
	 * @return a new {@link Action} whose values re-ordered using a PriorityQueue.
	 * @since 2.0
	 */
	public final Action<O, O> sort(int maxCapacity, Comparator<? super O> comparator) {
		return connect(new SortAction<O>(maxCapacity, dispatcher, comparator));
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined {@link this#getCapacity()}
	 * times. The nested streams will be pushed into the returned {@code Stream}.
	 *
	 * @return a new {@link Action} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final Action<O, Stream<O>> window() {
		return window(capacity);
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined {@param backlog} times.
	 * The nested streams will be pushed into the returned {@code Stream}.
	 *
	 * @param backlog the time period when each window close and flush the attached consumer
	 * @return a new {@link Action} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final Action<O, Stream<O>> window(long backlog) {
		return connect(new WindowAction<O>(dispatcher, backlog));
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} for each unique key evaluated by the
	 * {param keyMapper}.
	 *
	 * @param keyMapper the key mapping function that evaluates an incoming data and returns a key.
	 * @return a new {@link Action} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final <K> GroupByAction<O, K> groupBy(Function<? super O, ? extends K> keyMapper) {
		return connect(new GroupByAction<O, K>(keyMapper, dispatcher));
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} for each unique key evaluated by the
	 * {param keyMapper}. The hashcode of the incoming data will be used for partitioning
	 *
	 * @return a new {@link Action} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final GroupByAction<O, Integer> partition() {
		return groupBy(new Function<O, Integer>() {
			@Override
			public Integer apply(O o) {
				return o.hashCode();
			}
		});
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}. The given initial object will be
	 * passed to the function's {@link reactor.tuple.Tuple2} argument.
	 *
	 * @param fn      the reduce function
	 * @param initial the initial argument to pass to the reduce function
	 * @param <A>     the type of the reduced object
	 * @return a new {@link Action} whose values contain only the reduced objects
	 */
	public final <A> Action<O, A> reduce(A initial, @Nonnull Function<Tuple2<O, A>, A> fn) {
		return reduce(Functions.supplier(initial), capacity, fn);
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}. The given {@link Supplier} will be
	 * used to produce initial accumulator objects either on the first reduce call, in the case of an unbounded {@code
	 * Stream}, or on the first value of each batch, if a {@code capacity} is set.
	 * <p>
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
	 * @return a new {@link Action} whose values contain only the reduced objects
	 */
	public final <A> Action<O, A> reduce(@Nullable final Supplier<A> accumulators,
	                                     final long batchSize,
	                                     @Nonnull final Function<Tuple2<O, A>, A> fn
	) {

		return connect(new ReduceAction<O, A>(
				batchSize,
				accumulators,
				fn,
				dispatcher
		));
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}.
	 *
	 * @param fn  the reduce function
	 * @param <A> the type of the reduced object
	 * @return a new {@link Action} whose values contain only the reduced objects
	 */
	public final <A> Action<O, A> reduce(@Nonnull final Function<Tuple2<O, A>, A> fn) {
		return reduce(null, capacity, fn);
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}. The given initial object will be
	 * passed to the function's {@link Tuple2} argument. Behave like Reduce but triggers downstream Stream for every
	 * transformation.
	 *
	 * @param fn      the scan function
	 * @param initial the initial argument to pass to the reduce function
	 * @param <A>     the type of the reduced object
	 * @return a new {@link Action} whose values contain only the reduced objects
	 * @since 1.1, 2.0
	 */
	public final <A> Action<O, A> scan(@Nonnull Function<Tuple2<O, A>, A> fn, A initial) {
		return scan(fn, Functions.supplier(initial));
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}. The given {@link Supplier} will be
	 * used to produce initial accumulator objects either on the first reduce call, in the case of an unbounded {@code
	 * Stream}, or on the first value of each batch, if a {@code capacity} is set.
	 * <p>
	 * The accumulated value will be published on the returned {@code Stream} every time
	 * a
	 * value is accepted.
	 *
	 * @param fn           the scan function
	 * @param accumulators the {@link Supplier} that will provide accumulators
	 * @param <A>          the type of the reduced object
	 * @return a new {@link Action} whose values contain only the reduced objects
	 * @since 1.1, 2.0
	 */
	public final <A> Action<O, A> scan(@Nonnull final Function<Tuple2<O, A>, A> fn,
	                                   @Nullable final Supplier<A> accumulators) {
		return connect(new ScanAction<O, A>(accumulators,
				fn,
				dispatcher));
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}.
	 *
	 * @param fn  the reduce function
	 * @param <A> the type of the reduced object
	 * @return a new {@link Action} whose values contain only the reduced objects
	 * @since 1.1, 2.0
	 */
	public final <A> Action<O, A> scan(@Nonnull final Function<Tuple2<O, A>, A> fn) {
		return scan(fn, (Supplier<A>) null);
	}

	/**
	 * Count accepted events for each batch and pass each accumulated long to the {@param stream}.
	 */
	public final Action<O, Long> count() {
		return count(capacity);
	}

	/**
	 * Count accepted events for each batch {@param i} and pass each accumulated long to the {@param stream}.
	 *
	 * @return a new {@link Action}
	 */
	public final Action<O, Long> count(long i) {
		return connect(new CountAction<O>(dispatcher, i));
	}

	/**
	 * Request the parent stream every {@param period} milliseconds. Timeout is run on the environment root timer.
	 *
	 * @param period the period in milliseconds between two notifications on this stream
	 * @return a new {@link Action}
	 * @since 2.0
	 */
	public final Action<O, O> throttle(long period) {
		Assert.state(getEnvironment() != null, "Cannot use default timer as no environment has been provided to this " +
				"Stream");
		return throttle(period, 0l);
	}

	/**
	 * Request the parent stream every {@param period} milliseconds after an initial {@param delay}.
	 * Timeout is run on the environment root timer.
	 *
	 * @param delay  the timeout in milliseconds before starting consuming
	 * @param period the period in milliseconds between two notifications on this stream
	 * @return a new {@link Action}
	 * @since 2.0
	 */
	public final Action<O, O> throttle(long period, long delay) {
		Assert.state(getEnvironment() != null, "Cannot use default timer as no environment has been provided to this " +
				"Stream");
		return throttle(period, delay, getEnvironment().getRootTimer());
	}

	/**
	 * Request the parent stream every {@param period} milliseconds after an initial {@param delay}.
	 * Timeout is run on the given {@param timer}.
	 *
	 * @param period the timeout in milliseconds between two notifications on this stream
	 * @param delay  the timeout in milliseconds before starting consuming
	 * @param timer  the reactor timer to run the timeout on
	 * @return a new {@link Action}
	 * @since 2.0
	 */
	public final Action<O, O> throttle(long period, long delay, Timer timer) {
		return connect(new ThrottleAction<O>(
				dispatcher,
				timer,
				period,
				delay
		));
	}

	/**
	 * Request the parent stream when the last notification occurred after {@param
	 * timeout} milliseconds. Timeout is run on the environment root timer.
	 *
	 * @param timeout the timeout in milliseconds between two notifications on this composable
	 * @return a new {@link Action}
	 * @since 1.1, 2.0
	 */
	public final TimeoutAction<O> timeout(long timeout) {
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
	 * @return a new {@link Action}
	 * @since 1.1, 2.0
	 */
	public TimeoutAction<O> timeout(long timeout, Timer timer) {
		return connect(new TimeoutAction<O>(
				dispatcher,
				null,
				timer,
				timeout
		));
	}

	/**
	 * Create a consumer that broadcast complete signal from any accepted value.
	 *
	 * @return a new {@link Consumer} ready to forward complete signal to this stream
	 * @since 2.0
	 */
	public final Consumer<?> toBroadcastCompleteConsumer() {
		return new Consumer<Object>() {
			@Override
			public void accept(Object o) {
				broadcastComplete();
			}
		};
	}

	/**
	 * Create a consumer that broadcast next signal from accepted values.
	 *
	 * @return a new {@link Consumer} ready to forward values to this stream
	 * @since 2.0
	 */
	public final Consumer<O> toBroadcastNextConsumer() {
		return new Consumer<O>() {
			@Override
			public void accept(O o) {
				broadcastNext(o);
			}
		};
	}

	/**
	 * Create a consumer that broadcast error signal from any accepted value.
	 *
	 * @return a new {@link Consumer} ready to forward error to this stream
	 * @since 2.0
	 */
	public final Consumer<Throwable> toBroadcastErrorConsumer() {
		return new Consumer<Throwable>() {
			@Override
			public void accept(Throwable o) {
				broadcastError(o);
			}
		};
	}

	/**
	 * Return the promise of the next triggered signal.
	 * A promise is a container that will capture only once the first arriving error|next|complete signal
	 * to this {@link Stream}. It is useful to coordinate on single data streams or await for any signal.
	 *
	 * @return a new {@link Promise}
	 * @since 2.0
	 */
	public final Promise<O> next() {
		Promise<O> d = new Promise<O>(
				dispatcher,
				environment
		);
		subscribe(d);
		return d;
	}

	/**
	 * Blocking call to eagerly fetch values from this stream
	 *
	 * @return the buffered collection
	 * @since 2.0
	 */
	public final Promise<List<O>> toList() throws InterruptedException {
		return toList(-1);
	}

	/**
	 * Return the promise of N signals collected into an array list.
	 *
	 * @param maximum list size and therefore events signal to listen for
	 * @return the buffered collection
	 * @since 2.0
	 */
	public final Promise<List<O>> toList(long maximum) {
		if (maximum > 0)
			return limit(maximum).buffer().next();
		else {
			return buffer().next();
		}
	}

	/**
	 * Blocking call to pass values from this stream to the queue that can be polled from a consumer.
	 *
	 * @return the buffered queue
	 * @since 2.0
	 */
	public final CompletableBlockingQueue<O> toBlockingQueue() throws InterruptedException {
		return toBlockingQueue(-1);
	}

	/**
	 * Blocking call to eagerly fetch values from this stream
	 *
	 * @param maximum queue capacity, a full queue might block the stream producer.
	 * @return the buffered queue
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final CompletableBlockingQueue<O> toBlockingQueue(int maximum) throws InterruptedException {
		final CompletableBlockingQueue<O> blockingQueue;
		Stream<O> tail = this;
		if (maximum > 0) {
			blockingQueue = new CompletableBlockingQueue<O>(maximum);
			tail = limit(maximum);
		} else {
			blockingQueue = new CompletableBlockingQueue<O>(1);
		}

		Consumer terminalConsumer = new Consumer<Object>() {
			@Override
			public void accept(Object o) {
				blockingQueue.complete();
			}
		};

		TerminalCallbackAction<O> callbackAction = new TerminalCallbackAction<O>(dispatcher, new Consumer<O>() {
			@Override
			public void accept(O o) {
				try {
					blockingQueue.put(o);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}, terminalConsumer, terminalConsumer);

		tail.connect(callbackAction);

		return blockingQueue;
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
	public void recycle() {
		downstreamSubscription = null;
		state = State.READY;
		environment = null;
		keepAlive = false;
		error = null;
	}

	@Override
	public void subscribe(final Subscriber<? super O> subscriber) {
		checkAndSubscribe(subscriber, createSubscription(subscriber, true));
	}

	/**
	 * Subscribe the {@link reactor.rx.action.CombineAction#input()} to this Stream. Combining action through {@link
	 * reactor.rx.action.Action#combine()} allows for easy distribution of a full flow.
	 *
	 * @param subscriber the combined actions to subscribe
	 * @since 2.0
	 */
	public <A> void subscribe(final CombineAction<O, A> subscriber) {
		subscribe(subscriber.input());
	}

	/**
	 * Send an element of parameterized type {link O} to all the attached {@link Subscriber}.
	 * A Stream must be in READY state to dispatch signals and will fail fast otherwise (IllegalStateException).
	 *
	 * @param ev the data to forward
	 * @since 2.0
	 */
	public void broadcastNext(final O ev) {
		//log.debug("event [" + ev + "] by: " + getClass().getSimpleName());
		if (!checkState() || downstreamSubscription == null) {
			if (log.isTraceEnabled()) {
				log.trace("event [" + ev + "] dropped by: " + getClass().getSimpleName() + ":" + this);
			}
			return;
		}

		try {
			downstreamSubscription.onNext(ev);

			if (state == State.COMPLETE || (downstreamSubscription != null && downstreamSubscription.isComplete())) {
				downstreamSubscription.onComplete();
			}
		} catch (Throwable throwable) {
			callError(downstreamSubscription, throwable);
		}
	}

	/**
	 * Send an error to all the attached {@link Subscriber}.
	 * A Stream must be in READY state to dispatch signals and will fail fast otherwise (IllegalStateException).
	 *
	 * @param throwable the error to forward
	 * @since 2.0
	 */
	public void broadcastError(final Throwable throwable) {
		//log.debug("event [" + throwable + "] by: " + getClass().getSimpleName());
		if (!checkState()) {
			if (log.isTraceEnabled()) {
				log.trace("error dropped by: " + getClass().getSimpleName() + ":" + this, throwable);
			}
		}

		if (!ignoreErrors) {
			state = State.ERROR;
			error = throwable;
		}

		if (downstreamSubscription == null) {
			log.error(this.getClass().getSimpleName() + " > broadcastError:" + this, new Exception(debug().toString(),
					throwable));
			return;
		}

		downstreamSubscription.onError(throwable);
	}

	/**
	 * Send a complete event to all the attached {@link Subscriber} ONLY IF the underlying state is READY.
	 * Unlike {@link #broadcastNext(Object)} and {@link #broadcastError(Throwable)} it will simply ignore the signal.
	 *
	 * @since 2.0
	 */
	public void broadcastComplete() {
		//log.debug("event [complete] by: " + getClass().getSimpleName());
		if (state != State.READY) {
			if (log.isTraceEnabled()) {
				log.trace("Complete signal dropped by: " + getClass().getSimpleName() + ":" + this);
			}
			return;
		}

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

	protected void checkAndSubscribe(final Subscriber<? super O> subscriber, final StreamSubscription<O> subscription) {
		if (state == State.READY && addSubscription(subscription)) {
			if (subscription.asyncManaged()) {
				subscriber.onSubscribe(subscription);
			} else {
				dispatch(new Consumer<Void>() {
					@Override
					public void accept(Void aVoid) {
						subscriber.onSubscribe(subscription);
					}
				});
			}
		} else if (state == State.COMPLETE) {
			subscriber.onComplete();
		} else if (state == State.SHUTDOWN) {
			subscriber.onError(new IllegalStateException("Publisher has shutdown"));
		} else if (state == State.ERROR) {
			subscriber.onError(error);
		}
	}

	protected StreamSubscription<O> createSubscription(Subscriber<? super O> subscriber, boolean reactivePull) {
		if (reactivePull) {
			return new StreamSubscription<O>(this, subscriber);
		} else {
			return new StreamSubscription.Firehose<O>(this, subscriber);
		}
	}

	public StreamSubscription<O> downstreamSubscription() {
		return downstreamSubscription;
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
	 * @return long capacity for this {@link Stream}
	 */
	final public long getCapacity() {
		return capacity;
	}

	@SuppressWarnings("unchecked")
	protected boolean addSubscription(final StreamSubscription<O> subscription) {
		StreamSubscription<O> currentSubscription = this.downstreamSubscription;
		if (currentSubscription == null) {
			this.downstreamSubscription = subscription;
			return true;
		} else if (currentSubscription.equals(subscription)) {
			subscription.onError(SpecificationExceptions.spec_2_12_exception());
			return false;
		} else if (FanOutSubscription.class.isAssignableFrom(currentSubscription.getClass())) {
			if (((FanOutSubscription<O>) currentSubscription).contains(subscription)) {
				subscription.onError(SpecificationExceptions.spec_2_12_exception());
				return false;
			} else {
				return ((FanOutSubscription<O>) currentSubscription).add(subscription);
			}
		} else {
			this.downstreamSubscription = new FanOutSubscription<O>(this, currentSubscription, subscription);
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
		} else {
			StreamSubscription<O> dsub = this.downstreamSubscription;
			if (FanOutSubscription.class.isAssignableFrom(dsub.getClass())) {
				FanOutSubscription<O> fsub =
						((FanOutSubscription<O>) this.downstreamSubscription);

				if (fsub.remove(subscription) && fsub.isEmpty() && !keepAlive) {
					state = State.SHUTDOWN;
				}
			}
		}

	}

	protected void setState(State state) {
		this.state = state;
	}

	private void callError(StreamSubscription<O> subscription, Throwable cause) {
		subscription.onError(cause);
	}

	public final boolean checkState() {
		return state == State.READY;
	}

	public static enum State {
		READY,
		ERROR,
		COMPLETE,
		SHUTDOWN
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
				", ignoreErrors=" + ignoreErrors +
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
