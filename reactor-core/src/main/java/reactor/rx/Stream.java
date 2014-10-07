/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.rx;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Environment;
import reactor.core.Observable;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.selector.Selectors;
import reactor.function.*;
import reactor.function.support.Tap;
import reactor.queue.CompletableBlockingQueue;
import reactor.queue.CompletableQueue;
import reactor.rx.action.*;
import reactor.rx.action.support.DefaultSubscriber;
import reactor.rx.subscription.PushSubscription;
import reactor.timer.Timer;
import reactor.tuple.Tuple2;
import reactor.tuple.TupleN;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
 * Typically, new {@code Stream} aren't created directly. To create a {@code Stream},
 * create a {@link Streams} and configure it with the appropriate {@link Environment},
 * {@link Dispatcher}, and other settings.
 *
 * @param <O> The type of the output values
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1, 2.0
 */
public abstract class Stream<O> implements Publisher<O> {


	protected Stream() {
	}

	/**
	 * Cast the current Stream flowing data type into a target class type.
	 *
	 * @param <E> the {@link Action} output type
	 * @return the current {link Stream} instance casted
	 * @since 2.0
	 */
	@SuppressWarnings({"unchecked", "unused"})
	public final <E> Stream<E> cast(@Nonnull final Class<E> stream) {
		return (Stream<E>) this;
	}

	/**
	 * Subscribe an {@link Action} to the actual pipeline to produce events to (subscribe,error,complete,next)
	 *
	 * @param action the processor to subscribe.
	 *
	 * @return the current stream
	 * @see {@link org.reactivestreams.Publisher#subscribe(org.reactivestreams.Subscriber)}
	 * @since 2.0
	 */
	public final Stream<O> connectAnd(@Nonnull final Action<? super O, ?> action) {
		this.connect(action);
		return this;
	}

	/**
	 * Subscribe an {@link Action} to the actual pipeline.
	 * Additionally to producing events (subscribe,error,complete,next)
	 *
	 * Reactive Extensions patterns also dubs this operation "lift".
	 * The operation is returned for functional-style chaining.
	 *
	 * @param action the processor to subscribe.
	 * @param <E>    the {@link Action} type
	 * @param <A>    the {@link Action} output type
	 *
	 * @return the passed action
	 * @see {@link org.reactivestreams.Publisher#subscribe(org.reactivestreams.Subscriber)}
	 * @since 2.0
	 */
	public <A, E extends Action<? super O, ? extends A>> E connect(@Nonnull final E action) {
		this.subscribe(action);
		return action;
	}

	/**
	 * Subscribe an {@link Subscriber} to the actual pipeline. Additionally to producing events (error,complete,next,
	 * subscribe),
	 * Reactive Extensions patterns also dubs this operation "lift".
	 *
	 * @param subscriber the processor to subscribe.
	 * @param <E>    the {@link Subscriber} output type
	 * @return the passed subscriber
	 * @see {@link org.reactivestreams.Publisher#subscribe(org.reactivestreams.Subscriber)}
	 * @since 2.0
	 */
	public final <E extends Subscriber<? super O>> E connect(@Nonnull final E subscriber) {
		this.subscribe(subscriber);
		return subscriber;
	}

	/**
	 * Subscribe an {@link Subscriber} to the actual pipeline. Additionally to producing events (error,complete,next,
	 * subscribe),
	 *
	 * @param subscriber the processor to subscribe.
	 * @return the current {link Stream} instance
	 * @see {@link org.reactivestreams.Publisher#subscribe(org.reactivestreams.Subscriber)}
	 * @since 2.0
	 */
	public final Stream<O> subscribeAnd(@Nonnull final Subscriber<? super O> subscriber) {
		this.subscribe(subscriber);
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
	@SuppressWarnings("unchecked")
	public final <E extends Throwable> Action<O, O> when(@Nonnull final Class<E> exceptionType,
	                                                     @Nonnull final Consumer<E> onError) {
		return connect(new ErrorAction<O, E>(getDispatcher(), Selectors.T(exceptionType), onError));
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
		RecoverAction<O, E> recoverAction = new RecoverAction<O, E>(getDispatcher(), Selectors.T(exceptionType));
		return connect(recoverAction);
	}

	/**
	 * Instruct the action to request upstream subscription if any for {@link this#capacity} elements. If the dispatcher
	 * is asynchronous (RingBufferDispatcher for instance), it will proceed the request asynchronously as well.
	 *
	 * @return the consuming action
	 */
	public Action<O, Void> drain() {
		return consume(null);
	}

	/**
	 * Instruct the action to request upstream subscription if any for N elements.
	 *
	 * @return the current stream
	 */
	public Stream<O> drain(final long n) {
		this.subscribe(new DefaultSubscriber<O>() {
			@Override
			public void onSubscribe(Subscription s) {
				s.request(n);
			}
		});
		return this;
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
		return connect(new TerminalCallbackAction<O>(getDispatcher(), consumer, null, null));
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
		return connect(new TerminalCallbackAction<O>(getDispatcher(), consumer, errorConsumer, null));
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
		return connect(new TerminalCallbackAction<O>(getDispatcher(), consumer, errorConsumer, completeConsumer));
	}

	/**
	 * Assign a new Environment and its default Dispatcher to the returned Stream. If the dispatcher is different,
	 * the new action will take
	 * care of buffering incoming data into a StreamSubscription. Otherwise default behavior is picked:
	 * FireHose synchronous subscription is the parent stream != null
	 *
	 * @param environment the environment to get dispatcher from {@link reactor.core.Environment#getDefaultDispatcher()}
	 * @return a new {@link Action} running on a different {@link Dispatcher}
	 */
	public Action<?, O> dispatchOn(@Nonnull final Environment environment) {
		return dispatchOn(environment, environment.getDefaultDispatcher());
	}

	/**
	 * Assign a new Dispatcher to the returned Stream. If the dispatcher is different, the new action will take
	 * care of buffering incoming data into a StreamSubscription. Otherwise default behavior is picked:
	 * FireHose synchronous subscription is the parent stream != null
	 *
	 * @param dispatcher the new dispatcher
	 * @return a new {@link Action} running on a different {@link Dispatcher}
	 */
	public Action<?, O> dispatchOn(@Nonnull final Dispatcher dispatcher) {
		return dispatchOn(null, dispatcher);
	}

	/**
	 * Assign the a new Dispatcher and an Environment to the returned Stream. If the dispatcher is different,
	 * the new action will take
	 * care of buffering incoming data into a StreamSubscription. Otherwise default behavior is picked:
	 * FireHose synchronous subscription is the parent stream != null
	 *
	 * @param dispatcher  the new dispatcher
	 * @param environment the environment
	 * @return a new {@link Action} running on a different {@link Dispatcher}
	 */
	public Action<?, O> dispatchOn(final Environment environment, @Nonnull Dispatcher dispatcher) {
		Assert.state(dispatcher.supportsOrdering(), "Dispatcher provided doesn't support event ordering. " +
				" Refer to #parallel() method. ");
		return connect(Action.<O>passthrough(dispatcher, getCapacity()).env(environment));
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
		return connect(new CallbackAction<O>(getDispatcher(), consumer));
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will observe terminal signal complete|error. It will pass
	 * the
	 * newly created {@link Stream} to the consumer for state introspection, e.g. {@link Action#getFinalState()}
	 * Stream}.
	 *
	 * @param consumer the consumer to invoke on terminal signal
	 * @return {@literal this}
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <E> Action<O, O> finallyDo(Consumer<E> consumer) {
		return connect(new FinallyAction<O, E>(getDispatcher(), (E) this, consumer));
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
		return connect(new ObservableAction<O>(getDispatcher(), observable, key));
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
		return connect(new MapAction<O, V>(fn, getDispatcher()));
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
	 * offered by default StreamSubscription. If merge hasn't getCapacity() to take new elements because its {@link
	 * this#getCapacity()(long)} instructed so, the subscription will buffer
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
	public final Stream<O> mergeWith(Publisher<? extends O> publisher) {
		return Streams.merge(this, publisher).dispatchOn(getEnvironment(), getDispatcher());
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
	public final <V> Stream<List<V>> joinWith(Publisher<? extends V> publisher) {
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
		return fanIn(new ZipAction<Object, V, TupleN>(getDispatcher(), zipper, null));
	}

	/**
	 * {@link this#connect(Action)} with the passed {@link Publisher} values to a new {@link Stream} until one of them
	 * complete.
	 * The result will be produced by the zipper transformation from a tuple of each upstream most recent emitted data.
	 *
	 * @return the zipped stream
	 * @since 2.0
	 */
	public final <T2, V> Stream<V> zipWith(Publisher<? extends T2> publisher,
	                                       @Nonnull Function<Tuple2<O, T2>, V> zipper) {
		return Streams.zip(this, publisher, zipper).dispatchOn(getEnvironment(), getDispatcher());
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
	public final <T2, V> Stream<V> zipWith(Iterable<? extends T2> iterable,
	                                       @Nonnull Function<Tuple2<O, T2>, V> zipper) {
		return zipWith(Streams.defer(iterable), zipper);
	}

	/**
	 * {@link this#connect(Action)} all the nested {@link Publisher} values to a new {@link Stream} calling the logic
	 * inside the provided fanInAction for complex merging strategies.
	 * {@link reactor.rx.action.FanInAction} provides helpers to create subscriber for each source,
	 * a registry of incoming sources and overriding doXXX signals as usual to produce the result via
	 * reactor.rx.action.Action#broadcastXXX.
	 * <p>
	 * A default fanInAction will act like {@link this#merge()}, passing values to doNext. In java8 one can then
	 * implement
	 * stream.fanIn(data -> broadcastNext(data)) or stream.fanIn(System.out::println)
	 * <p>
	 * <p>
	 * Dynamic merge (moving nested data into the top-level returned stream) requires use of reactive-pull offered by
	 * default StreamSubscription. If merge hasn't getCapacity() to
	 * take new elements because its {@link
	 * this#getCapacity()(long)} instructed so, the subscription will buffer
	 * them.
	 *
	 * @param <T> the nested type of flowing upstream Stream.
	 * @param <V> the produced output
	 * @return the zipped stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public <T, V> Action<Publisher<? extends T>, V> fanIn(
			FanInAction<T, V, ? extends FanInAction.InnerSubscriber<T, V>> fanInAction
	) {
		Stream<Publisher<T>> thiz = (Stream<Publisher<T>>) this;

		Action<Publisher<? extends T>, V> innerMerge = new DynamicMergeAction<T, V>(getDispatcher(), fanInAction);
		innerMerge.capacity(getCapacity()).env(getEnvironment());

		thiz.subscribe(innerMerge);
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
	public final ConcurrentAction<O> parallel() {
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
	public final ConcurrentAction<O> parallel(final Integer poolsize) {
		return parallel(poolsize, getEnvironment() != null ?
				getEnvironment().getDefaultDispatcherFactory() :
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
	public final ConcurrentAction<O> parallel(Integer poolsize, final Supplier<Dispatcher> dispatcherSupplier) {
		return connect(new ConcurrentAction<O>(
				getDispatcher(), dispatcherSupplier, poolsize
		));
	}

	/**
	 * Attach a No-Op Action that only serves the purpose of buffering incoming values if not enough demand is signaled
	 * downstream. A buffering capable stream will prevent underlying getDispatcher() to be saturated (and sometimes
	 * blocking).
	 *
	 * @return a buffered stream
	 * @since 2.0
	 */
	public final Action<O, O> onOverflowBuffer() {
		return onOverflowBuffer(null);
	}

	/**
	 * Attach a No-Op Action that only serves the purpose of buffering incoming values if not enough demand is signaled
	 * downstream. A buffering capable stream will prevent underlying getDispatcher() to be saturated (and sometimes
	 * blocking).
	 *
	 * @param queue A completable queue {@link reactor.function.Supplier} to provide support for overflow
	 * @return a buffered stream
	 * @since 2.0
	 */
	public Action<O, O> onOverflowBuffer(CompletableQueue<O> queue) {
		return dispatchOn(getEnvironment(), getDispatcher()).onOverflowBuffer(queue);
	}

	/**
	 * Evaluate each accepted value against the given {@link Predicate}. If the predicate test succeeds, the value is
	 * passed into the new {@code Stream}. If the predicate test fails, the value is propagated into the {@link
	 * reactor.rx.action.FilterAction#otherwise()} composable .
	 *
	 * @param p the {@link Predicate} to test values against
	 * @return a new {@link Action} containing only values that pass the predicate test
	 */
	public final FilterAction<O> filter(final Predicate<? super O> p) {
		return connect(new FilterAction<O>(p, getDispatcher()));
	}

	/**
	 * Evaluate each accepted boolean value. If the predicate test succeeds, the value is
	 * passed into the new {@code Stream}. If the predicate test fails, the value is ignored.
	 *
	 * @return a new {@link Action} containing only values that pass the predicate test
	 * @since 1.1, 2.0
	 */
	@SuppressWarnings("unchecked")
	public final FilterAction<Boolean> filter() {
		return ((Stream<Boolean>) this).filter(FilterAction.simplePredicate);
	}

	/**
	 * Create a new {@code Stream} whose only value will be the current instance of the {@link Stream}.
	 *
	 * @return a new {@link Action} whose only value will be the materialized current {@link Stream}
	 * @since 2.0
	 */
	public final Stream<Stream<O>> nest() {
		return Streams.just(this);
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
		return connect(new RetryAction<O>(getDispatcher(), numRetries, retryMatcher));
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
	public final Action<O, O> limit(long max, Predicate<O> limitMatcher) {
		return connect(new LimitAction<O>(getDispatcher(), limitMatcher, max));
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
		return connect(new TimestampAction<O>(getDispatcher()));
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
		return connect(new ElapsedAction<O>(getDispatcher()));
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch. Requires a {@code
	 * getCapacity()}
	 * to
	 * have been set.
	 * <p>
	 * When a new batch is triggered, the first value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @return a new {@link Action} whose values are the first value of each batch
	 */
	public final Action<O, O> first() {
		return first(getCapacity());
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch. Requires a {@code
	 * getCapacity()}
	 * to
	 * have been set.
	 * <p>
	 * When a new batch is triggered, the first value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @param batchSize the batch size to use
	 * @return a new {@link Action} whose values are the first value of each batch)
	 */
	public final Action<O, O> first(long batchSize) {
		return connect(new FirstAction<O>(batchSize, getDispatcher()));
	}


	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code
	 * getCapacity()}
	 *
	 * @return a new {@link Action} whose values are the last value of each batch
	 */
	public final Action<O, O> last() {
		return every(getCapacity());
	}


	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code
	 * getCapacity()}
	 *
	 * @param batchSize the batch size to use
	 * @return a new {@link Action} whose values are the last value of each batch
	 */
	public final Action<O, O> every(long batchSize) {
		return connect(new LastAction<O>(batchSize, getDispatcher()));
	}

	/**
	 * Create a new {@code Stream} that filters out consecutive equals values.
	 *
	 * @return a new {@link Action} whose values are the last value of each batch
	 * @since 2.0
	 */
	public final Action<O, O> distinctUntilChanged() {
		final DistinctUntilChangedAction<O> d = new DistinctUntilChangedAction<O>(getDispatcher());
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
		return iterableStream.connect(new SplitAction<V>(getDispatcher()).capacity(batchSize));
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
		connect(new TerminalCallbackAction<O>(getDispatcher(), tap, null, null));
		return tap;
	}

	/**
	 * Collect incoming values into a {@link java.util.List} that will be pushed into the returned {@code Stream} every
	 * time {@code
	 * getCapacity()} or flush is triggered has been reached.
	 *
	 * @return a new {@link Action} whose values are a {@link java.util.List} of all values in this batch
	 */
	public final Action<O, List<O>> buffer() {
		return buffer(getCapacity());
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
	 * getCapacity()} has been reached.
	 *
	 * @param batchSize the collected size
	 * @return a new {@link Action} whose values are a {@link List} of all values in this batch
	 */
	public final Action<O, List<O>> buffer(long batchSize) {
		return connect(new BufferAction<O>(batchSize, getDispatcher()));
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
		return connect(new MovingBufferAction<O>(getDispatcher(), backlog, 1));
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
		return sort((int) getCapacity(), comparator);
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
		return connect(new SortAction<O>(maxCapacity, getDispatcher(), comparator));
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined {@link this#getCapacity()}
	 * times. The nested streams will be pushed into the returned {@code Stream}.
	 *
	 * @return a new {@link Action} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final Action<O, Stream<O>> window() {
		return window(getCapacity());
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
		return connect(new WindowAction<O>(getDispatcher(), backlog));
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
		return connect(new GroupByAction<O, K>(keyMapper, getDispatcher()));
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
		return reduce(Functions.supplier(initial), getCapacity(), fn);
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}. The given {@link Supplier} will be
	 * used to produce initial accumulator objects either on the first reduce call, in the case of an unbounded {@code
	 * Stream}, or on the first value of each batch, if a {@code getCapacity()} is set.
	 * <p>
	 * In an unbounded {@code Stream}, the accumulated value will be published on the returned {@code Stream} on flush
	 * only. But when a {@code getCapacity()} has been, the accumulated
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
				getDispatcher()
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
		return reduce(null, getCapacity(), fn);
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}. The given initial object will be
	 * passed to the function's {@link Tuple2} argument. Behave like Reduce but triggers downstream Stream for every
	 * transformation.
	 *
	 * @param initial the initial argument to pass to the reduce function
	 * @param fn      the scan function
	 * @param <A>     the type of the reduced object
	 * @return a new {@link Action} whose values contain only the reduced objects
	 * @since 1.1, 2.0
	 */
	public final <A> Action<O, A> scan(A initial, @Nonnull Function<Tuple2<O, A>, A> fn) {
		return scan(Functions.supplier(initial), fn);
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
		return scan((Supplier<A>) null, fn);
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}. The given {@link Supplier} will be
	 * used to produce initial accumulator objects either on the first reduce call, in the case of an unbounded {@code
	 * Stream}, or on the first value of each batch, if a {@code getCapacity()} is set.
	 * <p>
	 * The accumulated value will be published on the returned {@code Stream} every time
	 * a
	 * value is accepted.
	 *
	 * @param accumulators the {@link Supplier} that will provide accumulators
	 * @param fn           the scan function
	 * @param <A>          the type of the reduced object
	 * @return a new {@link Action} whose values contain only the reduced objects
	 * @since 1.1, 2.0
	 */
	public final <A> Action<O, A> scan(@Nullable final Supplier<A> accumulators,
	                                   @Nonnull final Function<Tuple2<O, A>, A> fn) {
		return connect(new ScanAction<O, A>(accumulators,
				fn,
				getDispatcher()));
	}

	/**
	 * Count accepted events for each batch and pass each accumulated long to the {@param stream}.
	 */
	public final Action<O, Long> count() {
		return count(getCapacity());
	}

	/**
	 * Count accepted events for each batch {@param i} and pass each accumulated long to the {@param stream}.
	 *
	 * @return a new {@link Action}
	 */
	public final Action<O, Long> count(long i) {
		return connect(new CountAction<O>(getDispatcher(), i));
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
		return throttle(period, delay, getEnvironment().getTimer());
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
				getDispatcher(),
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
		return timeout(timeout, getEnvironment().getTimer());
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
				getDispatcher(),
				null,
				timer,
				timeout
		));
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
				getDispatcher(),
				getEnvironment()
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
	 * @param maximum queue getCapacity(), a full queue might block the stream producer.
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

		TerminalCallbackAction<O> callbackAction = new TerminalCallbackAction<O>(getDispatcher(), new Consumer<O>() {
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
	 * @since 2.0
	 */
	public StreamUtils.StreamVisitor debug() {
		return StreamUtils.browse(this);
	}

	/**
	 * Subscribe the {@link reactor.rx.action.CombineAction#input()} to this Stream. Combining action through {@link
	 * reactor.rx.action.Action#combine()} allows for easy distribution of a full flow.
	 *
	 * @param subscriber the combined actions to subscribe
	 * @since 2.0
	 */
	public final <A> void subscribe(final CombineAction<O, A> subscriber) {
		subscribe(subscriber.input());
	}

	/**
	 * Get the assigned {@link reactor.event.dispatch.Dispatcher}.
	 *
	 * @return current {@link reactor.event.dispatch.Dispatcher}
	 */
	public Dispatcher getDispatcher() {
		return SynchronousDispatcher.INSTANCE;
	}

	/**
	 * Return defined {@link Stream} capacity, used to drive new {@link org.reactivestreams.Subscription}
	 * request needs.
	 *
	 * @return long capacity for this {@link Stream}
	 */
	public long getCapacity() {
		return Long.MAX_VALUE;
	}

	/**
	 * Get the assigned {@link reactor.core.Environment}.
	 *
	 * @return current {@link Environment}
	 */
	public Environment getEnvironment() {
		return null;
	}

	/**
	 * Get the current action child subscription
	 *
	 * @return current child {@link reactor.rx.subscription.PushSubscription}
	 */
	public PushSubscription<O> downstreamSubscription() {
		return null;
	}

	/**
	 * Try cleaning a given subscription from the stream references. Unicast implementation such as IterableStream
	 * (Streams.defer(1,2,3)) or SupplierStream (Streams.generate(-> 1)) won't need to perform any job and as such will
	 * return @{code false} upon this call.
	 * Alternatively, Action and HotStream (Streams.defer()) will clean any reference to that subscription from their
	 * internal registry and might return {@code true} if successful.
	 *
	 * @return current child {@link reactor.rx.subscription.PushSubscription}
	 */
	public boolean cleanSubscriptionReference(PushSubscription<O> oPushSubscription) {
		return false;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
}
