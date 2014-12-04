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
import reactor.core.Environment;
import reactor.core.Observable;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.selector.Selector;
import reactor.function.Function;
import reactor.function.Supplier;
import reactor.rx.action.*;
import reactor.rx.stream.*;
import reactor.timer.Timer;
import reactor.tuple.*;
import reactor.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A public factory to build {@link Stream}.
 * <p>
 * Examples of use (In Java8 but would also work with Anonymous classes or Groovy Closures for instance):
 * {@code
 * Streams.just(1, 2, 3).map(i -> i*2) //...
 * <p>
 * Stream<String> stream = Streams.from().map(i -> i*2).consume(System.out::println);
 * stream.broadcastNext("hello");
 * <p>
 * Stream.create( subscriber -> {
 * subscriber.onNext(1);
 * subscriber.onNext(2);
 * subscriber.onNext(3);
 * subscriber.onComplete();
 * }).consume(System.out::println);
 * <p>
 * Stream<Integer> inputStream1 = Streams.from(env);
 * Stream<Integer> inputStream2 = Streams.from(env);
 * Stream.merge(environment, inputStream1, inputStream2).map(i -> i*2).consume(System.out::println);
 * <p>
 * }
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
public final class Streams {

	/**
	 * Build a synchronous {@literal Stream} that will only emit a complete signal to any new subscriber.
	 *
	 * @return a new {@link Stream}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stream<T> empty() {
		return (Stream<T>)SingleValueStream.EMPTY;
	}

	/**
	 * Build a synchronous {@literal Stream} that will only emit an error signal to any new subscriber.
	 *
	 * @return a new {@link Stream}
	 */
	public static <O, T extends Throwable> Stream<O> fail(T throwable) {
		return new ErrorStream<O, T>(throwable);
	}


	/**
	 * Build a synchronous {@literal Stream}, ready to broadcast values with {@link reactor.rx.action
	 * .Action#broadcastNext
	 * (Object)},
	 * {@link reactor.rx.action.Action#broadcastError(Throwable)}, {@link reactor.rx.action.Action#broadcastComplete()}.
	 *
	 * @param <T> the type of values passing through the {@literal action}
	 * @return a new {@link Action}
	 */
	public static <T> Broadcaster<T> broadcast() {
		return Action.<T>passthrough(SynchronousDispatcher.INSTANCE).keepAlive();
	}


	/**
	 * Build a {@literal Stream}, ready to broadcast values, ready to broadcast values with {@link
	 * reactor.rx.action.Action#broadcastNext(Object)},
	 * {@link reactor.rx.action.Action#broadcastError(Throwable)}, {@link reactor.rx.action.Action#broadcastComplete()}.
	 *
	 * @param env the Reactor {@link reactor.core.Environment} to use
	 * @param <T> the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Broadcaster<T> broadcast(Environment env) {
		return broadcast(env, env.getDefaultDispatcher());
	}


	/**
	 * Build a {@literal Stream}, ready to broadcast values with {@link reactor.rx.action.Action#broadcastNext
	 * (Object)},
	 * {@link reactor.rx.action.Action#broadcastError(Throwable)}, {@link reactor.rx.action.Action#broadcastComplete()}.
	 *
	 * @param env        the Reactor {@link reactor.core.Environment} to use
	 * @param dispatcher the {@link reactor.event.dispatch.Dispatcher} to use
	 * @param <T>        the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Broadcaster<T> broadcast(Environment env, Dispatcher dispatcher) {
		Assert.state(dispatcher.supportsOrdering(), "Dispatcher provided doesn't support event ordering. " +
				" For concurrent consume, refer to #partition()/groupBy() method and assign individual single dispatchers");
		Broadcaster<T> broadcaster = Action.<T>passthrough(dispatcher);
		broadcaster.env(env).capacity(dispatcher.backlogSize() > 0 ?
				(Action.RESERVED_SLOTS > dispatcher.backlogSize() ?
						dispatcher.backlogSize() :
						dispatcher.backlogSize() - Action.RESERVED_SLOTS) :
				Long.MAX_VALUE);
		return broadcaster.keepAlive();
	}

	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription request.
	 * <p>
	 * It will use the passed dispatcher to emit signals.
	 *
	 * @param values The values to {@code on()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> Stream<T> from(Iterable<? extends T> values) {
		return new IterableStream<T>(values);
	}

	/**
	 * Build a {@literal Stream} that will only emit the result of the future and then complete.
	 * The future will be polled for an unbounded amount of time.
	 *
	 * @param future the future to poll value from
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> from(Future<? extends T> future) {
		return new FutureStream<T>(future);
	}


	/**
	 * Build a {@literal Stream} that will only emit the result of the future and then complete.
	 * The future will be polled for an unbounded amount of time.
	 *
	 * @param future the future to poll value from
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> from(Future<? extends T> future, long time, TimeUnit unit) {
		return new FutureStream<T>(future, time, unit);
	}

	/**
	 * Build a {@literal Stream} that will only emit a sequence of integers within the specified range and then
	 * complete.
	 *
	 * @param start the inclusive starting value to be emitted
	 * @param end   the inclusive closing value to be emitted
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Integer> range(int start, int end) {
		return new RangeStream(start, end);
	}


	/**
	 * Build a {@literal Stream} that will only emit 0l after the time delay and then complete.
	 *
	 * @param delay the timespan in SECONDS to wait before emitting 0l and complete signals
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> timer(long delay) {
		return timer(Environment.timer(), delay, TimeUnit.SECONDS);
	}


	/**
	 * Build a {@literal Stream} that will only emit 0l after the time delay and then complete.
	 *
	 * @param timer the timer to run on
	 * @param delay the timespan in SECONDS to wait before emitting 0l and complete signals
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> timer(Timer timer, long delay) {
		return timer(timer, delay, TimeUnit.SECONDS);
	}


	/**
	 * Build a {@literal Stream} that will only emit 0l after the time delay and then complete.
	 *
	 * @param delay the timespan in [unit] to wait before emitting 0l and complete signals
	 * @param unit  the time unit
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> timer(long delay, TimeUnit unit) {
		return new SingleTimerStream(delay, unit, Environment.timer());
	}

	/**
	 * Build a {@literal Stream} that will only emit 0l after the time delay and then complete.
	 *
	 * @param timer the timer to run on
	 * @param delay the timespan in [unit] to wait before emitting 0l and complete signals
	 * @param unit  the time unit
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> timer(Timer timer, long delay, TimeUnit unit) {
		return new SingleTimerStream(delay, unit, timer);
	}

	/**
	 * Build a {@literal Stream} that will emit ever increasing counter from 0 after on each period from the subscribe call.
	 * It will never complete until cancelled.
	 *
	 * @param period the period in SECONDS before each following increment
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> period(long period) {
		return period(Environment.timer(), -1l, period, TimeUnit.SECONDS);
	}


	/**
	 * Build a {@literal Stream} that will emit ever increasing counter from 0 after on each period from the subscribe call.
	 * It will never complete until cancelled.
	 *
	 * @param timer the timer to run on
	 * @param period the period in SECONDS before each following increment
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> period(Timer timer, long period) {
		return period(timer, -1l, period, TimeUnit.SECONDS);
	}


	/**
	 * Build a {@literal Stream} that will emit ever increasing counter from 0 after the time delay on each period.
	 * It will never complete until cancelled.
	 *
	 * @param delay the timespan in SECONDS to wait before emitting 0l
	 * @param period the period in SECONDS before each following increment
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> period(long delay, long period) {
		return period(Environment.timer(), delay, period, TimeUnit.SECONDS);
	}


	/**
	 * Build a {@literal Stream} that will emit ever increasing counter from 0 after the time delay on each period.
	 * It will never complete until cancelled.
	 *
	 * @param timer the timer to run on
	 * @param delay the timespan in SECONDS to wait before emitting 0l
	 * @param period the period in SECONDS before each following increment
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> period(Timer timer, long delay, long period) {
		return period(timer, delay, period, TimeUnit.SECONDS);
	}

	/**
	 * Build a {@literal Stream} that will emit ever increasing counter from 0 after the subscribe call on each period.
	 * It will never complete until cancelled.
	 *
	 * @param period the period in [unit] before each following increment
	 * @param unit  the time unit
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> period(long period, TimeUnit unit) {
		return period(Environment.timer(), -1l, period, unit);
	}

	/**
	 * Build a {@literal Stream} that will emit ever increasing counter from 0 after the subscribe call on each period.
	 * It will never complete until cancelled.
	 *
	 * @param timer the timer to run on
	 * @param period the period in [unit] before each following increment
	 * @param unit  the time unit
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> period(Timer timer, long period, TimeUnit unit) {
		return period(timer, -1l, period, unit);
	}

	/**
	 * Build a {@literal Stream} that will emit ever increasing counter from 0 after the subscribe call on each period.
	 * It will never complete until cancelled.
	 *
	 * @param delay the timespan in [unit] to wait before emitting 0l
	 * @param period the period in [unit] before each following increment
	 * @param unit  the time unit
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> period(long delay, long period, TimeUnit unit) {
		return period(Environment.timer(), delay, period, unit);
	}

	/**
	 * Build a {@literal Stream} that will emit ever increasing counter from 0 after the time delay on each period.
	 * It will never complete until cancelled.
	 *
	 * @param timer the timer to run on
	 * @param delay the timespan in [unit] to wait before emitting 0l
	 * @param period the period in [unit] before each following increment
	 * @param unit  the time unit
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> period(Timer timer, long delay, long period, TimeUnit unit) {
		return new PeriodicTimerStream(TimeUnit.MILLISECONDS.convert(delay, unit), period, unit, timer);
	}



	/**
	 * Build a synchronous {@literal Stream} whom data is sourced by the passed element on subscription
	 * request. After all data is being dispatched, a complete signal will be emitted.
	 * <p>
	 *
	 * @param value1 The only value to {@code onNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> Stream<T> just(T value1) {
		return new SingleValueStream<T>(value1);
	}


	/**
	 * Build a synchronous {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
	 * request.
	 * <p>
	 *
	 * @param value1 The first value to {@code onNext()}
	 * @param value2 The second value to {@code onNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> Stream<T> just(T value1, T value2) {
		return from(Arrays.asList(value1, value2));
	}


	/**
	 * Build a synchronous {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
	 * request.
	 * <p>
	 *
	 * @param value1 The first value to {@code onNext()}
	 * @param value2 The second value to {@code onNext()}
	 * @param value3 The third value to {@code onNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> Stream<T> just(T value1, T value2, T value3) {
		return from(Arrays.asList(value1, value2, value3));
	}


	/**
	 * Build a synchronous {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
	 * request.
	 * <p>
	 *
	 * @param value1 The first value to {@code onNext()}
	 * @param value2 The second value to {@code onNext()}
	 * @param value3 The third value to {@code onNext()}
	 * @param value4 The fourth value to {@code onNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> Stream<T> just(T value1, T value2, T value3, T value4) {
		return from(Arrays.asList(value1, value2, value3, value4));
	}


	/**
	 * Build a synchronous {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
	 * request.
	 * <p>
	 *
	 * @param value1 The first value to {@code onNext()}
	 * @param value2 The second value to {@code onNext()}
	 * @param value3 The third value to {@code onNext()}
	 * @param value4 The fourth value to {@code onNext()}
	 * @param value5 The fifth value to {@code onNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> Stream<T> just(T value1, T value2, T value3, T value4, T value5) {
		return from(Arrays.asList(value1, value2, value3, value4, value5));
	}


	/**
	 * Build a synchronous {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
	 * request.
	 * <p>
	 *
	 * @param value1 The first value to {@code onNext()}
	 * @param value2 The second value to {@code onNext()}
	 * @param value3 The third value to {@code onNext()}
	 * @param value4 The fourth value to {@code onNext()}
	 * @param value5 The fifth value to {@code onNext()}
	 * @param value6 The sixth value to {@code onNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> Stream<T> just(T value1, T value2, T value3, T value4, T value5, T value6) {
		return from(Arrays.asList(value1, value2, value3, value4, value5, value6));
	}


	/**
	 * Build a synchronous {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
	 * request.
	 * <p>
	 *
	 * @param value1 The first value to {@code onNext()}
	 * @param value2 The second value to {@code onNext()}
	 * @param value3 The third value to {@code onNext()}
	 * @param value4 The fourth value to {@code onNext()}
	 * @param value5 The fifth value to {@code onNext()}
	 * @param value6 The sixth value to {@code onNext()}
	 * @param value7 The seventh value to {@code onNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> Stream<T> just(T value1, T value2, T value3, T value4, T value5, T value6, T value7) {
		return from(Arrays.asList(value1, value2, value3, value4, value5, value6, value7));
	}


	/**
	 * Build a synchronous {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
	 * request.
	 * <p>
	 *
	 * @param value1 The first value to {@code onNext()}
	 * @param value2 The second value to {@code onNext()}
	 * @param value3 The third value to {@code onNext()}
	 * @param value4 The fourth value to {@code onNext()}
	 * @param value5 The fifth value to {@code onNext()}
	 * @param value6 The sixth value to {@code onNext()}
	 * @param value7 The seventh value to {@code onNext()}
	 * @param value8 The eigth value to {@code onNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> Stream<T> just(T value1, T value2, T value3, T value4, T value5, T value6, T value7, T value8) {
		return from(Arrays.asList(value1, value2, value3, value4, value5, value6, value7, value8));
	}

	/**
	 * Build a synchronous {@literal Stream}, ready to broadcast values from the given publisher. A publisher will start
	 * producing next elements until onComplete is called.
	 *
	 * @param publisher the publisher to broadcast the Stream subscriber
	 * @param <T>       the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> create(Publisher<T> publisher) {
		return new PublisherStream<T>(publisher);
	}

	/**
	 * Attach a synchronous Stream to the {@link Observable} with the specified {@link Selector}.
	 *
	 * @param observable        the {@link Observable} to observe
	 * @param broadcastSelector the {@link Selector}/{@literal Object} tuple to listen to
	 * @param <T>               the type of values passing through the {@literal Stream}
	 * @return a new {@link Stream}
	 * @since 2.0
	 */
	public static <T> Stream<T> on(Observable observable, Selector broadcastSelector) {
		return new ObservableStream<T>(observable, broadcastSelector);
	}

	/**
	 * Build a synchronous {@literal Stream} whose data is generated by the passed supplier on subscription request.
	 * The Stream's batch size will be set to 1.
	 *
	 * @param value The value to {@code on()}
	 * @param <T>   type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> generate(Supplier<? extends T> value) {
		if (value == null) throw new IllegalArgumentException("Supplier must be provided");
		return new SupplierStream<T>(value);
	}


	/**
	 * Build a Synchronous {@literal Stream} whose data are emitted by the most recent passed publisher.
	 * The stream will complete once both the publishers source and the last switched to publisher have completed.
	 *
	 * @param mergedPublishers The publisher of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>              type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Action<? extends Publisher<? extends T>, T> switchOnNext(Publisher<? extends Publisher<? extends T>> mergedPublishers) {
		final Action<Publisher<? extends T>, T> mergeAction = new SwitchAction<>(SynchronousDispatcher.INSTANCE);

		mergedPublishers.subscribe(mergeAction);
		return mergeAction;
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed to.
	 *
	 * @param mergedPublishers The list of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>              type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> concat(Iterable<? extends Publisher<? extends T>> mergedPublishers) {
		final List<Publisher<? extends T>> publishers = new ArrayList<>();
		for (Publisher<? extends T> mergedPublisher : mergedPublishers) {
			publishers.add(mergedPublisher);
		}
		return new ConcatAction<T>(SynchronousDispatcher.INSTANCE, publishers);
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed to.
	 *
	 * @param concatdPublishers The publisher of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>              type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> concat(Publisher<? extends Publisher<? extends T>> concatdPublishers) {
		final Action<Publisher<? extends T>, T> concatAction = new DynamicMergeAction<T, T>(SynchronousDispatcher.INSTANCE,
				new ConcatAction<T>(SynchronousDispatcher.INSTANCE, null)
		);

		concatdPublishers.subscribe(concatAction);
		return concatAction;
	}


	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed to.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> concat(Publisher<? extends T> source1,
	                                  Publisher<? extends T> source2
	) {
		return concat(Arrays.asList(source1, source2));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed to.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> concat(Publisher<? extends T> source1,
	                                  Publisher<? extends T> source2,
	                                  Publisher<? extends T> source3
	) {
		return concat(Arrays.asList(source1, source2, source3));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed to.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> concat(Publisher<? extends T> source1,
	                                  Publisher<? extends T> source2,
	                                  Publisher<? extends T> source3,
	                                  Publisher<? extends T> source4
	) {
		return concat(Arrays.asList(source1, source2, source3, source4));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed to.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> concat(Publisher<? extends T> source1,
	                                  Publisher<? extends T> source2,
	                                  Publisher<? extends T> source3,
	                                  Publisher<? extends T> source4,
	                                  Publisher<? extends T> source5
	) {
		return concat(Arrays.asList(source1, source2, source3, source4, source5));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed to.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> concat(Publisher<? extends T> source1,
	                                  Publisher<? extends T> source2,
	                                  Publisher<? extends T> source3,
	                                  Publisher<? extends T> source4,
	                                  Publisher<? extends T> source5,
	                                  Publisher<? extends T> source6
	) {
		return concat(Arrays.asList(source1, source2, source3, source4, source5,
				source6));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed to.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source7 The seventh upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> concat(Publisher<? extends T> source1,
	                                  Publisher<? extends T> source2,
	                                  Publisher<? extends T> source3,
	                                  Publisher<? extends T> source4,
	                                  Publisher<? extends T> source5,
	                                  Publisher<? extends T> source6,
	                                  Publisher<? extends T> source7
	) {
		return concat(Arrays.asList(source1, source2, source3, source4, source5,
				source6, source7));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed to.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source7 The seventh upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source8 The eigth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> concat(Publisher<? extends T> source1,
	                                  Publisher<? extends T> source2,
	                                  Publisher<? extends T> source3,
	                                  Publisher<? extends T> source4,
	                                  Publisher<? extends T> source5,
	                                  Publisher<? extends T> source6,
	                                  Publisher<? extends T> source7,
	                                  Publisher<? extends T> source8
	) {
		return concat(Arrays.asList(source1, source2, source3, source4, source5,
				source6, source7, source8));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param mergedPublishers The list of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>              type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> merge(Iterable<? extends Publisher<? extends T>> mergedPublishers) {
		final List<Publisher<? extends T>> publishers = new ArrayList<>();
		for (Publisher<? extends T> mergedPublisher : mergedPublishers) {
			publishers.add(mergedPublisher);
		}
		return new MergeAction<T>(SynchronousDispatcher.INSTANCE, publishers);
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param mergedPublishers The publisher of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>              type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T, E extends T> Stream<E> merge(Publisher<? extends Publisher<E>> mergedPublishers) {
		final Action<Publisher<? extends E>, E> mergeAction = new DynamicMergeAction<E, E>(SynchronousDispatcher.INSTANCE,
				new MergeAction<E>(SynchronousDispatcher.INSTANCE, null)
		);

		mergedPublishers.subscribe(mergeAction);
		return mergeAction;
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> merge(Publisher<? extends T> source1,
	                                     Publisher<? extends T> source2
	) {
		return merge(Arrays.asList(source1, source2));
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> merge(Publisher<? extends T> source1,
	                                     Publisher<? extends T> source2,
	                                     Publisher<? extends T> source3
	) {
		return merge(Arrays.asList(source1, source2, source3));
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> merge(Publisher<? extends T> source1,
	                                     Publisher<? extends T> source2,
	                                     Publisher<? extends T> source3,
	                                     Publisher<? extends T> source4
	) {
		return merge(Arrays.asList(source1, source2, source3, source4));
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> merge(Publisher<? extends T> source1,
	                                     Publisher<? extends T> source2,
	                                     Publisher<? extends T> source3,
	                                     Publisher<? extends T> source4,
	                                     Publisher<? extends T> source5
	) {
		return merge(Arrays.asList(source1, source2, source3, source4, source5));
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> merge(Publisher<? extends T> source1,
	                                     Publisher<? extends T> source2,
	                                     Publisher<? extends T> source3,
	                                     Publisher<? extends T> source4,
	                                     Publisher<? extends T> source5,
	                                     Publisher<? extends T> source6
	) {
		return merge(Arrays.asList(source1, source2, source3, source4, source5,
				source6));
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source7 The seventh upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> merge(Publisher<? extends T> source1,
	                                     Publisher<? extends T> source2,
	                                     Publisher<? extends T> source3,
	                                     Publisher<? extends T> source4,
	                                     Publisher<? extends T> source5,
	                                     Publisher<? extends T> source6,
	                                     Publisher<? extends T> source7
	) {
		return merge(Arrays.asList(source1, source2, source3, source4, source5,
				source6, source7));
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source7 The seventh upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source8 The eigth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> merge(Publisher<? extends T> source1,
	                                     Publisher<? extends T> source2,
	                                     Publisher<? extends T> source3,
	                                     Publisher<? extends T> source4,
	                                     Publisher<? extends T> source5,
	                                     Publisher<? extends T> source6,
	                                     Publisher<? extends T> source7,
	                                     Publisher<? extends T> source8
	) {
		return merge(Arrays.asList(source1, source2, source3, source4, source5,
				source6, source7, source8));
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param zipper  The aggregate function that will receive a unique value from each upstream and return the
	 *                value to signal downstream
	 * @param <T1>    type of the value from source1
	 * @param <T2>    type of the value from source2
	 * @param <V>     The produced output after transformation by {@param zipper}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, V> Stream<V> zip(Publisher<? extends T1> source1,
	                                           Publisher<? extends T2> source2,
	                                           Function<Tuple2<T1, T2>, ? extends V> zipper) {
		return zip(Arrays.asList(source1, source2), zipper);
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param zipper  The aggregate function that will receive a unique value from each upstream and return the
	 *                value to signal downstream
	 * @param <T1>    type of the value from source1
	 * @param <T2>    type of the value from source2
	 * @param <T3>    type of the value from source3
	 * @param <V>     The produced output after transformation by {@param zipper}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, V> Stream<V> zip(Publisher<? extends T1> source1,
	                                               Publisher<? extends T2> source2,
	                                               Publisher<? extends T3> source3,
	                                               Function<Tuple3<T1, T2, T3>,
			                                               ? extends V> zipper) {
		return zip(Arrays.asList(source1, source2, source3), zipper);
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param zipper  The aggregate function that will receive a unique value from each upstream and return the
	 *                value to signal downstream
	 * @param <T1>    type of the value from source1
	 * @param <T2>    type of the value from source2
	 * @param <T3>    type of the value from source3
	 * @param <T4>    type of the value from source4
	 * @param <V>     The produced output after transformation by {@param zipper}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, T4, V> Stream<V> zip(Publisher<? extends T1> source1,
	                                                   Publisher<? extends T2> source2,
	                                                   Publisher<? extends T3> source3,
	                                                   Publisher<? extends T4> source4,
	                                                   Function<Tuple4<T1, T2, T3, T4>,
			                                                   V> zipper) {
		return zip(Arrays.asList(source1, source2, source3, source4), zipper);
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param zipper  The aggregate function that will receive a unique value from each upstream and return the
	 *                value to signal downstream
	 * @param <T1>    type of the value from source1
	 * @param <T2>    type of the value from source2
	 * @param <T3>    type of the value from source3
	 * @param <T4>    type of the value from source4
	 * @param <T5>    type of the value from source5
	 * @param <V>     The produced output after transformation by {@param zipper}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, T4, T5, V> Stream<V> zip(Publisher<? extends T1> source1,
	                                                       Publisher<? extends T2> source2,
	                                                       Publisher<? extends T3> source3,
	                                                       Publisher<? extends T4> source4,
	                                                       Publisher<? extends T5> source5,
	                                                       Function<Tuple5<T1, T2, T3, T4, T5>,
			                                                       V> zipper) {
		return zip(Arrays.asList(source1, source2, source3, source4, source5), zipper);
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param zipper  The aggregate function that will receive a unique value from each upstream and return the
	 *                value to signal downstream
	 * @param <T1>    type of the value from source1
	 * @param <T2>    type of the value from source2
	 * @param <T3>    type of the value from source3
	 * @param <T4>    type of the value from source4
	 * @param <T5>    type of the value from source5
	 * @param <T6>    type of the value from source6
	 * @param <V>     The produced output after transformation by {@param zipper}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, T4, T5, T6, V> Stream<V> zip(Publisher<? extends T1> source1,
	                                                           Publisher<? extends T2> source2,
	                                                           Publisher<? extends T3> source3,
	                                                           Publisher<? extends T4> source4,
	                                                           Publisher<? extends T5> source5,
	                                                           Publisher<? extends T6> source6,
	                                                           Function<Tuple6<T1, T2, T3, T4, T5, T6>,
			                                                           V> zipper) {
		return zip(Arrays.asList(source1, source2, source3, source4, source5, source6), zipper);
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source7 The seventh upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T1>    type of the value from source1
	 * @param <T2>    type of the value from source2
	 * @param <T3>    type of the value from source3
	 * @param <T4>    type of the value from source4
	 * @param <T5>    type of the value from source5
	 * @param <T6>    type of the value from source6
	 * @param <T7>    type of the value from source7
	 * @param zipper  The aggregate function that will receive a unique value from each upstream and return the
	 *                value to signal downstream
	 * @param <V>     The produced output after transformation by {@param zipper}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, V> Stream<V> zip(Publisher<? extends T1> source1,
	                                                               Publisher<? extends T2> source2,
	                                                               Publisher<? extends T3> source3,
	                                                               Publisher<? extends T4> source4,
	                                                               Publisher<? extends T5> source5,
	                                                               Publisher<? extends T6> source6,
	                                                               Publisher<? extends T7> source7,
	                                                               Function<Tuple7<T1, T2, T3, T4, T5, T6, T7>,
			                                                               V> zipper) {
		return zip(Arrays.asList(source1, source2, source3, source4, source5, source6, source7),
				zipper);
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source7 The seventh upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source8 The eigth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param zipper  The aggregate function that will receive a unique value from each upstream and return the
	 *                value to signal downstream
	 * @param <T1>    type of the value from source1
	 * @param <T2>    type of the value from source2
	 * @param <T3>    type of the value from source3
	 * @param <T4>    type of the value from source4
	 * @param <T5>    type of the value from source5
	 * @param <T6>    type of the value from source6
	 * @param <T7>    type of the value from source7
	 * @param <T8>    type of the value from source8
	 * @param <V>     The produced output after transformation by {@param zipper}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, T8, V> Stream<V> zip(Publisher<? extends T1> source1,
	                                                                   Publisher<? extends T2> source2,
	                                                                   Publisher<? extends T3> source3,
	                                                                   Publisher<? extends T4> source4,
	                                                                   Publisher<? extends T5> source5,
	                                                                   Publisher<? extends T6> source6,
	                                                                   Publisher<? extends T7> source7,
	                                                                   Publisher<? extends T8> source8,
	                                                                   Function<Tuple8<T1, T2, T3, T4, T5, T6, T7, T8>,
			                                                                   ? extends V> zipper) {
		return zip(Arrays.asList(source1, source2, source3, source4, source5, source6, source7, source8), zipper);
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param sources The list of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param zipper  The aggregate function that will receive a unique value from each upstream and return the
	 *                value to signal downstream
	 * @param <V>     The produced output after transformation by {@param zipper}
	 * @param <TUPLE> The type of tuple to use that must match source Publishers type
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <TUPLE extends Tuple, V> Stream<V> zip(Iterable<? extends Publisher<?>> sources,
	                                                        Function<TUPLE, ? extends V> zipper) {
		return new ZipAction<>(SynchronousDispatcher.INSTANCE, zipper, sources);
	}

	/**
	 * Build a synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param sources The publisher of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param zipper  The aggregate function that will receive a unique value from each upstream and return the
	 *                value to signal downstream
	 * @param <V>     The produced output after transformation by {@param zipper}
	 * @param <E>     The inner type of {@param source}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <E, TUPLE extends Tuple, V> Stream<V> zip(
			Publisher<? extends Publisher<E>> sources,
			Function<TUPLE, ? extends V> zipper)
	{
		final  Action<Publisher<? extends E>, V>  mergeAction = new DynamicMergeAction<E, V>(SynchronousDispatcher.INSTANCE,
				new ZipAction<E, V, TUPLE>(SynchronousDispatcher.INSTANCE, zipper, null)
		);

		sources.subscribe(mergeAction);

		return mergeAction;
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are aggregated from the passed publishers
	 * (1 element consumed for each merged publisher. resulting in an array of size of {@param mergedPublishers}.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<List<T>> join(Publisher<? extends T> source1,
	                                       Publisher<? extends T> source2) {
		return join(Arrays.asList(source1, source2));
	}


	/**
	 * Build a Synchronous {@literal Stream} whose data are aggregated from the passed publishers
	 * (1 element consumed for each merged publisher. resulting in an array of size of {@param mergedPublishers}.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<List<T>> join(Publisher<? extends T> source1,
	                                       Publisher<? extends T> source2,
	                                       Publisher<? extends T> source3) {
		return join(Arrays.asList(source1, source2, source3));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are aggregated from the passed publishers
	 * (1 element consumed for each merged publisher. resulting in an array of size of {@param mergedPublishers}.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<List<T>> join(Publisher<? extends T> source1,
	                                       Publisher<? extends T> source2,
	                                       Publisher<? extends T> source3,
	                                       Publisher<? extends T> source4) {
		return join(Arrays.asList(source1, source2, source3, source4));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are aggregated from the passed publishers
	 * (1 element consumed for each merged publisher. resulting in an array of size of {@param mergedPublishers}.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<List<T>> join(Publisher<? extends T> source1,
	                                       Publisher<? extends T> source2,
	                                       Publisher<? extends T> source3,
	                                       Publisher<? extends T> source4,
	                                       Publisher<? extends T> source5) {
		return join(Arrays.asList(source1, source2, source3, source4, source5));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are aggregated from the passed publishers
	 * (1 element consumed for each merged publisher. resulting in an array of size of {@param mergedPublishers}.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<List<T>> join(Publisher<? extends T> source1,
	                                       Publisher<? extends T> source2,
	                                       Publisher<? extends T> source3,
	                                       Publisher<? extends T> source4,
	                                       Publisher<? extends T> source5,
	                                       Publisher<? extends T> source6) {
		return join(Arrays.asList(source1, source2, source3, source4, source5,
				source6));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are aggregated from the passed publishers
	 * (1 element consumed for each merged publisher. resulting in an array of size of {@param mergedPublishers}.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<List<T>> join(Publisher<? extends T> source1,
	                                       Publisher<? extends T> source2,
	                                       Publisher<? extends T> source3,
	                                       Publisher<? extends T> source4,
	                                       Publisher<? extends T> source5,
	                                       Publisher<? extends T> source6,
	                                       Publisher<? extends T> source7) {
		return join(Arrays.asList(source1, source2, source3, source4, source5, source6, source7));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are aggregated from the passed publishers
	 * (1 element consumed for each merged publisher. resulting in an array of size of {@param mergedPublishers}.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5 The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6 The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source7 The seventh upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source8 The eigth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<List<T>> join(Publisher<? extends T> source1,
	                                       Publisher<? extends T> source2,
	                                       Publisher<? extends T> source3,
	                                       Publisher<? extends T> source4,
	                                       Publisher<? extends T> source5,
	                                       Publisher<? extends T> source6,
	                                       Publisher<? extends T> source7,
	                                       Publisher<? extends T> source8) {
		return join(Arrays.asList(source1, source2, source3, source4, source5, source6, source7, source8));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are aggregated from the passed publishers
	 * (1 element consumed for each merged publisher. resulting in an array of size of {@param mergedPublishers}.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param sources The list of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stream<List<T>> join(Iterable<? extends Publisher<? extends T>> sources) {
		return (Action<T, List<T>>)zip(sources, ZipAction.<TupleN, T>joinZipper());
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are aggregated from the passed publishers
	 * (1 element consumed for each merged publisher. resulting in an array of size of {@param mergedPublishers}.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source The publisher of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>    type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<List<T>> join(Publisher<? extends Publisher<T>> source) {
		return zip(source, ZipAction.<TupleN, T>joinZipper());
	}

}
