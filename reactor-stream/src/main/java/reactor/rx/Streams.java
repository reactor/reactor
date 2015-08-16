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
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.reactivestreams.PublisherFactory;
import reactor.core.reactivestreams.SubscriberWithContext;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.fn.timer.Timer;
import reactor.fn.tuple.*;
import reactor.rx.action.Action;
import reactor.rx.action.combination.*;
import reactor.rx.action.support.DefaultSubscriber;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.stream.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A public factory to build {@link Stream}, Streams provide for common transformations from a few structures such as
 * Iterable or Future to a Stream, in addition to provide for combinatory operations (merge, switchOnNext...).
 * <p>
 * <p>
 * Examples of use (In Java8 but would also work with Anonymous classes or Groovy Closures for instance):
 * <pre>
 * {@code
 * Streams.just(1, 2, 3).map(i -> i*2) //...
 *
 * Broadcaster<String> stream = Streams.broadcast()
 * strean.map(i -> i*2).consume(System.out::println);
 * stream.onNext("hello");
 *
 * Stream.create( subscriber -> {
 *   subscriber.onNext(1);
 *   subscriber.onNext(2);
 *   subscriber.onNext(3);
 *   subscriber.onComplete();
 * }).consume(System.out::println);
 *
 * Broadcaster<Integer> inputStream1 = Broadcaster.create(env);
 * Broadcaster<Integer> inputStream2 = Broadcaster.create(env);
 * Streams.merge(environment, inputStream1, inputStream2).map(i -> i*2).consume(System.out::println);
 *
 * }
 * </pre>
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 */
public class Streams {

	protected Streams() {
	}

	/**
	 * Build a custom sequence {@literal Stream} from the passed {@link org.reactivestreams.Publisher} that will be
	 * subscribed on the
	 * first
	 * request from the new subscriber. It means that the passed {@link org.reactivestreams.Subscription#request(long)}
	 * manually triggered or automatically consumed by {@link reactor.rx.Stream#consume()} operations. The sequence
	 * consists
	 * of a series of calls to the {@link org.reactivestreams.Subscriber} argument:
	 * onSubscribe?|onNext*|onError?|onComplete.
	 * Strict application of this protocol is not enforced, e.g. onSubscribe is not required as a buffering subscription
	 * will be created
	 * anyway.
	 * For simply decorating a given Publisher with {@link Stream} API, and thus relying on the publisher to honour the
	 * Reactive Streams protocol,
	 * use the {@link Streams#wrap(Publisher)}
	 *
	 * @param publisher the publisher to accept the Stream subscriber
	 * @param <T>       the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> create(Publisher<T> publisher) {
		if (Stream.class.isAssignableFrom(publisher.getClass())) {
			return (Stream<T>) publisher;
		}
		return new PublisherStream<T>(publisher);
	}


	/**
	 * Create a {@link Stream} reacting on requests with the passed {@link BiConsumer}
	 *
	 * @param requestConsumer A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param <T>             The type of the data sequence
	 * @return a Stream
	 * @since 2.0.2
	 */
	public static <T> Stream<T> createWith(BiConsumer<Long, SubscriberWithContext<T, Void>> requestConsumer) {
		return createWith(requestConsumer, null, null);
	}

	/**
	 * Create a {@link Stream} reacting on requests with the passed {@link BiConsumer}
	 * The argument {@code contextFactory} is executed once by new subscriber to generate a context shared by every
	 * request calls.
	 *
	 * @param requestConsumer A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param contextFactory  A {@link Function} called for every new subscriber returning an immutable context (IO
	 *                         connection...)
	 * @param <T>             The type of the data sequence
	 * @param <C>             The type of contextual information to be read by the requestConsumer
	 * @return a Stream
	 * @since 2.0.2
	 */
	public static <T, C> Stream<T> createWith(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
	                                          Function<Subscriber<? super T>, C> contextFactory) {
		return createWith(requestConsumer, contextFactory, null);
	}


	/**
	 * Create a {@link Stream} reacting on requests with the passed {@link BiConsumer}.
	 * The argument {@code contextFactory} is executed once by new subscriber to generate a context shared by every
	 * request calls.
	 * The argument {@code shutdownConsumer} is executed once by subscriber termination event (cancel, onComplete,
	 * onError).
	 *
	 * @param requestConsumer  A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param contextFactory   A {@link Function} called once for every new subscriber returning an immutable context
	 *                          (IO connection...)
	 * @param shutdownConsumer A {@link Consumer} called once everytime a subscriber terminates: cancel, onComplete(),
	 *                          onError()
	 * @param <T>              The type of the data sequence
	 * @param <C>              The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 * @since 2.0.2
	 */
	public static <T, C> Stream<T> createWith(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
	                                          Function<Subscriber<? super T>, C> contextFactory,
	                                          Consumer<C> shutdownConsumer) {

		return Streams.wrap(PublisherFactory.create(requestConsumer, contextFactory, shutdownConsumer));
	}

	/**
	 * A simple decoration of the given {@link Publisher} to expose {@link Stream} API and proxy any subscribe call to
	 * the publisher.
	 * The Publisher has to first call onSubscribe and receive a subscription request callback before any onNext call or
	 * will risk loosing events.
	 *
	 * @param publisher the publisher to decorate the Stream subscriber
	 * @param <T>       the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> wrap(final Publisher<T> publisher) {
		if (Stream.class.isAssignableFrom(publisher.getClass())) {
			return (Stream<T>) publisher;
		}
		return new Stream<T>() {
			@Override
			public void subscribe(Subscriber<? super T> s) {
				publisher.subscribe(s);
			}
		};
	}


	/**
	 * Supply a {@link Publisher} everytime subscribe is called on the returned stream. The passed {@link reactor.fn
	 * .Supplier}
	 * will be invoked and it's up to the developer to choose to return a new instance of a {@link Publisher} or reuse
	 * one,
	 * effecitvely behaving like {@link reactor.rx.Streams#wrap(Publisher)}.
	 *
	 * @param supplier the publisher factory to call on subscribe
	 * @param <T>      the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> defer(Supplier<? extends Publisher<T>> supplier) {
		return new DeferredStream<>(supplier);
	}

	/**
	 * Build a {@literal Stream} that will only emit a complete signal to any new subscriber.
	 *
	 * @return a new {@link Stream}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stream<T> empty() {
		return (Stream<T>) SingleValueStream.EMPTY;
	}


	/**
	 * Build a {@literal Stream} that will never emit anything.
	 *
	 * @return a new {@link Stream}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stream<T> never() {
		return (Stream<T>) NEVER;
	}


	/**
	 * Build a {@literal Stream} that will only emit an error signal to any new subscriber.
	 *
	 * @return a new {@link Stream}
	 */
	public static <O, T extends Throwable> Stream<O> fail(T throwable) {
		return new ErrorStream<O, T>(throwable);
	}


	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription request.
	 * <p>
	 * It will use the passed dispatcher to emit signals.
	 *
	 * @param values The values to {@code onNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> Stream<T> from(Iterable<? extends T> values) {
		return IterableStream.create(values);
	}

	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed array on subscription request.
	 * <p>
	 * It will use the passed dispatcher to emit signals.
	 *
	 * @param values The values to {@code onNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> Stream<T> from(T... values) {
		return IterableStream.create(Arrays.asList(values));
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
	 * Build a {@literal Stream} that will only emit a sequence of longs within the specified range and then
	 * complete.
	 *
	 * @param start the inclusive starting value to be emitted
	 * @param end   the inclusive closing value to be emitted
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> range(long start, long end) {
		return RangeStream.create(start, end);
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
	 * Build a {@literal Stream} that will emit ever increasing counter from 0 after on each period from the subscribe
	 * call.
	 * It will never complete until cancelled.
	 *
	 * @param period the period in SECONDS before each following increment
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> period(long period) {
		return period(Environment.timer(), -1l, period, TimeUnit.SECONDS);
	}


	/**
	 * Build a {@literal Stream} that will emit ever increasing counter from 0 after on each period from the subscribe
	 * call.
	 * It will never complete until cancelled.
	 *
	 * @param timer  the timer to run on
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
	 * @param delay  the timespan in SECONDS to wait before emitting 0l
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
	 * @param timer  the timer to run on
	 * @param delay  the timespan in SECONDS to wait before emitting 0l
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
	 * @param unit   the time unit
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> period(long period, TimeUnit unit) {
		return period(Environment.timer(), -1l, period, unit);
	}

	/**
	 * Build a {@literal Stream} that will emit ever increasing counter from 0 after the subscribe call on each period.
	 * It will never complete until cancelled.
	 *
	 * @param timer  the timer to run on
	 * @param period the period in [unit] before each following increment
	 * @param unit   the time unit
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> period(Timer timer, long period, TimeUnit unit) {
		return period(timer, -1l, period, unit);
	}

	/**
	 * Build a {@literal Stream} that will emit ever increasing counter from 0 after the subscribe call on each period.
	 * It will never complete until cancelled.
	 *
	 * @param delay  the timespan in [unit] to wait before emitting 0l
	 * @param period the period in [unit] before each following increment
	 * @param unit   the time unit
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> period(long delay, long period, TimeUnit unit) {
		return period(Environment.timer(), delay, period, unit);
	}

	/**
	 * Build a {@literal Stream} that will emit ever increasing counter from 0 after the time delay on each period.
	 * It will never complete until cancelled.
	 *
	 * @param timer  the timer to run on
	 * @param delay  the timespan in [unit] to wait before emitting 0l
	 * @param period the period in [unit] before each following increment
	 * @param unit   the time unit
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> period(Timer timer, long delay, long period, TimeUnit unit) {
		return new PeriodicTimerStream(TimeUnit.MILLISECONDS.convert(delay, unit), period, unit, timer);
	}


	/**
	 * Build a {@literal Stream} whom data is sourced by the passed element on subscription
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
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
	 * request.
	 * <p>
	 *
	 * @param value1 The first value to {@code onNext()}
	 * @param value2 The second value to {@code onNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> Stream<T> just(T value1, T value2) {
		return from(value1, value2);
	}


	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
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
		return from(value1, value2, value3);
	}


	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
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
		return from(value1, value2, value3, value4);
	}


	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
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
		return from(value1, value2, value3, value4, value5);
	}


	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
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
		return from(value1, value2, value3, value4, value5, value6);
	}


	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
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
		return from(value1, value2, value3, value4, value5, value6, value7);
	}


	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
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
		return from(value1, value2, value3, value4, value5, value6, value7, value8);
	}

	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
	 * request. Evoked only when there are more than 8 parameters.
	 * <p>
	 *
	 * @param values Send values to {@code onNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> Stream<T> just(T... values) {
		return from(values);
	}


	/**
	 * Build a {@literal Stream} whose data is generated by the passed supplier on subscription request.
	 * The Stream's batch size will be set to 1.
	 *
	 * @param value The value to {@code onNext()}
	 * @param <T>   type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> generate(Supplier<? extends T> value) {
		if (value == null) throw new IllegalArgumentException("Supplier must be provided");
		return new SupplierStream<T>(SynchronousDispatcher.INSTANCE, value);
	}

	/**
	 * Build a Synchronous {@literal Action} whose data are emitted by the most recent {@link Action#onNext(Object)}
	 * signaled publisher.
	 * The stream will complete once both the publishers source and the last switched to publisher have completed.
	 *
	 * @param <T> type of the value
	 * @return a {@link Action} accepting publishers and producing inner data T
	 * @since 2.0
	 */
	public static <T> Action<Publisher<? extends T>, T> switchOnNext() {
		return switchOnNext(SynchronousDispatcher.INSTANCE);
	}

	/**
	 * Build an {@literal Action} whose data are emitted by the most recent {@link Action#onNext(Object)} signaled
	 * publisher.
	 * The stream will complete once both the publishers source and the last switched to publisher have completed.
	 *
	 * @param dispatcher The dispatcher to execute the signals
	 * @param <T>        type of the value
	 * @return a {@link Action} accepting publishers and producing inner data T
	 * @since 2.0
	 */
	public static <T> Action<Publisher<? extends T>, T> switchOnNext(Dispatcher dispatcher) {
		SwitchAction<T> switchAction = new SwitchAction<>(dispatcher);
		switchAction.onSubscribe(Broadcaster.HOT_SUBSCRIPTION);
		return switchAction;
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
	public static <T> Stream<T> switchOnNext(
			Publisher<? extends Publisher<? extends T>> mergedPublishers) {
		return switchOnNext(mergedPublishers, SynchronousDispatcher.INSTANCE);
	}

	/**
	 * Build a {@literal Stream} whose data are emitted by the most recent passed publisher.
	 * The stream will complete once both the publishers source and the last switched to publisher have completed.
	 *
	 * @param mergedPublishers The publisher of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param dispatcher       The dispatcher to execute the signals
	 * @param <T>              type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> switchOnNext(
			Publisher<? extends Publisher<? extends T>> mergedPublishers, Dispatcher dispatcher) {
		final Action<Publisher<? extends T>, T> mergeAction = new SwitchAction<>(dispatcher);

		mergedPublishers.subscribe(mergeAction);
		return mergeAction;
	}


	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed
	 * to.
	 *
	 * @param mergedPublishers The list of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>              type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stream<T> concat(Iterable<? extends Publisher<? extends T>> mergedPublishers) {
		final List<Publisher<? extends T>> publishers = new ArrayList<>();
		for (Publisher<? extends T> mergedPublisher : mergedPublishers) {
			publishers.add(mergedPublisher);
		}
		int size = publishers.size();
		if (size == 1) {
			return Streams.wrap((Publisher<T>) publishers.get(0));
		} else if (size == 0) {
			return empty();
		}
		ConcatAction<T> concatAction = new ConcatAction<T>();
		from(mergedPublishers).subscribe(concatAction);
		return concatAction;
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed
	 * to.
	 *
	 * @param concatdPublishers The publisher of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>               type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> concat(Publisher<? extends Publisher<? extends T>> concatdPublishers) {
		final Action<Publisher<? extends T>, T> concatAction = new ConcatAction<>();
		concatdPublishers.subscribe(concatAction);
		return concatAction;
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed
	 * to.
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
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed
	 * to.
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
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed
	 * to.
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
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed
	 * to.
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
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed
	 * to.
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
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed
	 * to.
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
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been passed
	 * to.
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
	@SuppressWarnings("unchecked")
	public static <T> Stream<T> merge(Iterable<? extends Publisher<? extends T>> mergedPublishers) {
		final List<Publisher<? extends T>> publishers = new ArrayList<>();
		for (Publisher<? extends T> mergedPublisher : mergedPublishers) {
			publishers.add(mergedPublisher);
		}
		if (publishers.size() == 0) {
			return empty();
		} else if (publishers.size() == 1) {
			return wrap((Publisher<T>) publishers.get(0));
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
		final Action<Publisher<? extends E>, E> mergeAction = new DynamicMergeAction<E, E>(null);

		mergedPublishers.subscribe(mergeAction);
		return mergeAction;
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
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
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
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
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
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
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
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
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
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
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
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
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
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
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, V> Stream<V> combineLatest(Publisher<? extends T1> source1,
	                                                  Publisher<? extends T2> source2,
	                                                  Function<Tuple2<T1, T2>, ? extends V> combinator) {
		return combineLatest(Arrays.asList(source1, source2), combinator);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, V> Stream<V> combineLatest(Publisher<? extends T1> source1,
	                                                      Publisher<? extends T2> source2,
	                                                      Publisher<? extends T3> source3,
	                                                      Function<Tuple3<T1, T2, T3>,
			                                                      ? extends V> combinator) {
		return combineLatest(Arrays.asList(source1, source2, source3), combinator);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4    The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <T4>       type of the value from source4
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, T4, V> Stream<V> combineLatest(Publisher<? extends T1> source1,
	                                                          Publisher<? extends T2> source2,
	                                                          Publisher<? extends T3> source3,
	                                                          Publisher<? extends T4> source4,
	                                                          Function<Tuple4<T1, T2, T3, T4>,
			                                                          V> combinator) {
		return combineLatest(Arrays.asList(source1, source2, source3, source4), combinator);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4    The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <T4>       type of the value from source4
	 * @param <T5>       type of the value from source5
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, T4, T5, V> Stream<V> combineLatest(Publisher<? extends T1> source1,
	                                                              Publisher<? extends T2> source2,
	                                                              Publisher<? extends T3> source3,
	                                                              Publisher<? extends T4> source4,
	                                                              Publisher<? extends T5> source5,
	                                                              Function<Tuple5<T1, T2, T3, T4, T5>,
			                                                              V> combinator) {
		return combineLatest(Arrays.asList(source1, source2, source3, source4, source5), combinator);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4    The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5    The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6    The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <T4>       type of the value from source4
	 * @param <T5>       type of the value from source5
	 * @param <T6>       type of the value from source6
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, T4, T5, T6, V> Stream<V> combineLatest(Publisher<? extends T1> source1,
	                                                                  Publisher<? extends T2> source2,
	                                                                  Publisher<? extends T3> source3,
	                                                                  Publisher<? extends T4> source4,
	                                                                  Publisher<? extends T5> source5,
	                                                                  Publisher<? extends T6> source6,
	                                                                  Function<Tuple6<T1, T2, T3, T4, T5, T6>,
			                                                                  V> combinator) {
		return combineLatest(Arrays.asList(source1, source2, source3, source4, source5, source6), combinator);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4    The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5    The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6    The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source7    The seventh upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <T4>       type of the value from source4
	 * @param <T5>       type of the value from source5
	 * @param <T6>       type of the value from source6
	 * @param <T7>       type of the value from source7
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, V> Stream<V> combineLatest(Publisher<? extends T1> source1,
	                                                                      Publisher<? extends T2> source2,
	                                                                      Publisher<? extends T3> source3,
	                                                                      Publisher<? extends T4> source4,
	                                                                      Publisher<? extends T5> source5,
	                                                                      Publisher<? extends T6> source6,
	                                                                      Publisher<? extends T7> source7,
	                                                                      Function<Tuple7<T1, T2, T3, T4, T5, T6, T7>,
			                                                                      V> combinator) {
		return combineLatest(Arrays.asList(source1, source2, source3, source4, source5, source6, source7),
				combinator);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4    The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5    The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6    The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source7    The seventh upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source8    The eigth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <T4>       type of the value from source4
	 * @param <T5>       type of the value from source5
	 * @param <T6>       type of the value from source6
	 * @param <T7>       type of the value from source7
	 * @param <T8>       type of the value from source8
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, T4, T5, T6, T7, T8, V> Stream<V> combineLatest(Publisher<? extends T1> source1,
	                                                                          Publisher<? extends T2> source2,
	                                                                          Publisher<? extends T3> source3,
	                                                                          Publisher<? extends T4> source4,
	                                                                          Publisher<? extends T5> source5,
	                                                                          Publisher<? extends T6> source6,
	                                                                          Publisher<? extends T7> source7,
	                                                                          Publisher<? extends T8> source8,
	                                                                          Function<Tuple8<T1, T2, T3, T4, T5, T6,
			                                                                          T7, T8>,
			                                                                          ? extends V> combinator) {
		return combineLatest(Arrays.asList(source1, source2, source3, source4, source5, source6, source7, source8),
				combinator);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param sources    The list of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @param <TUPLE>    The type of tuple to use that must match source Publishers type
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <TUPLE extends Tuple, V> Stream<V> combineLatest(List<? extends Publisher<?>> sources,
	                                                               Function<TUPLE, ? extends V> combinator) {
		return new CombineLatestAction<>(SynchronousDispatcher.INSTANCE, combinator, sources);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param sources    The publisher of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @param <E>        The inner type of {@param source}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <E, TUPLE extends Tuple, V> Stream<V> combineLatest(
			Publisher<? extends Publisher<E>> sources,
			Function<TUPLE, ? extends V> combinator) {
		final Action<Publisher<? extends E>, V> mergeAction = new DynamicMergeAction<E, V>(
				new CombineLatestAction<E, V, TUPLE>(SynchronousDispatcher.INSTANCE, combinator, null)
		);

		sources.subscribe(mergeAction);

		return mergeAction;
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, V> Stream<V> zip(Publisher<? extends T1> source1,
	                                        Publisher<? extends T2> source2,
	                                        Function<Tuple2<T1, T2>, ? extends V> combinator) {
		return zip(Arrays.asList(source1, source2), combinator);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, V> Stream<V> zip(Publisher<? extends T1> source1,
	                                            Publisher<? extends T2> source2,
	                                            Publisher<? extends T3> source3,
	                                            Function<Tuple3<T1, T2, T3>,
			                                            ? extends V> combinator) {
		return zip(Arrays.asList(source1, source2, source3), combinator);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4    The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <T4>       type of the value from source4
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, T4, V> Stream<V> zip(Publisher<? extends T1> source1,
	                                                Publisher<? extends T2> source2,
	                                                Publisher<? extends T3> source3,
	                                                Publisher<? extends T4> source4,
	                                                Function<Tuple4<T1, T2, T3, T4>,
			                                                V> combinator) {
		return zip(Arrays.asList(source1, source2, source3, source4), combinator);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4    The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <T4>       type of the value from source4
	 * @param <T5>       type of the value from source5
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T1, T2, T3, T4, T5, V> Stream<V> zip(Publisher<? extends T1> source1,
	                                                    Publisher<? extends T2> source2,
	                                                    Publisher<? extends T3> source3,
	                                                    Publisher<? extends T4> source4,
	                                                    Publisher<? extends T5> source5,
	                                                    Function<Tuple5<T1, T2, T3, T4, T5>,
			                                                    V> combinator) {
		return zip(Arrays.asList(source1, source2, source3, source4, source5), combinator);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4    The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5    The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6    The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <T4>       type of the value from source4
	 * @param <T5>       type of the value from source5
	 * @param <T6>       type of the value from source6
	 * @param <V>        The produced output after transformation by {@param combinator}
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
			                                                        V> combinator) {
		return zip(Arrays.asList(source1, source2, source3, source4, source5, source6), combinator);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4    The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5    The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6    The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source7    The seventh upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <T4>       type of the value from source4
	 * @param <T5>       type of the value from source5
	 * @param <T6>       type of the value from source6
	 * @param <T7>       type of the value from source7
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <V>        The produced output after transformation by {@param combinator}
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
			                                                            V> combinator) {
		return zip(Arrays.asList(source1, source2, source3, source4, source5, source6, source7),
				combinator);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4    The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5    The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6    The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source7    The seventh upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source8    The eigth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <T4>       type of the value from source4
	 * @param <T5>       type of the value from source5
	 * @param <T6>       type of the value from source6
	 * @param <T7>       type of the value from source7
	 * @param <T8>       type of the value from source8
	 * @param <V>        The produced output after transformation by {@param combinator}
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
			                                                                ? extends V> combinator) {
		return zip(Arrays.asList(source1, source2, source3, source4, source5, source6, source7, source8), combinator);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param sources    The list of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @param <TUPLE>    The type of tuple to use that must match source Publishers type
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <TUPLE extends Tuple, V> Stream<V> zip(List<? extends Publisher<?>> sources,
	                                                     Function<TUPLE, ? extends V> combinator) {
		return new ZipAction<>(SynchronousDispatcher.INSTANCE, combinator, sources);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.
	 * The Stream's batch size will be set to {@literal Long.MAX_VALUE} or the minimum capacity allocated to any
	 * eventual {@link Stream} publisher type.
	 *
	 * @param sources    The publisher of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @param <E>        The inner type of {@param source}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <E, TUPLE extends Tuple, V> Stream<V> zip(
			Publisher<? extends Publisher<E>> sources,
			Function<TUPLE, ? extends V> combinator) {
		final Action<Publisher<? extends E>, V> mergeAction = new DynamicMergeAction<E, V>(
				new ZipAction<E, V, TUPLE>(SynchronousDispatcher.INSTANCE, combinator, null)
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
	public static <T> Stream<List<T>> join(List<? extends Publisher<? extends T>> sources) {
		return (Action<T, List<T>>) zip(sources, ZipAction.<TupleN, T>joinZipper());
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


	/**
	 * Wait 30 Seconds until a terminal signal from the passed publisher has been emitted.
	 * If the terminal signal is an error, it will propagate to the caller.
	 * Effectively this is making sure a stream has completed before the return of this call.
	 * It is usually used in controlled environment such as tests.
	 *
	 * @param publisher the publisher to listen for terminal signals
	 */
	public static void await(Publisher<?> publisher) throws Throwable {
		long timeout = 30000l;
		if (Environment.alive()) {
			timeout = Environment.get().getLongProperty("reactor.await.defaultTimeout", 30000L);
		}
		await(publisher, timeout, TimeUnit.MILLISECONDS, true);
	}

	/**
	 * Wait {code timeout} Seconds until a terminal signal from the passed publisher has been emitted.
	 * If the terminal signal is an error, it will propagate to the caller.
	 * Effectively this is making sure a stream has completed before the return of this call.
	 * It is usually used in controlled environment such as tests.
	 *
	 * @param publisher the publisher to listen for terminal signals
	 * @param timeout   the maximum wait time in seconds
	 */
	public static void await(Publisher<?> publisher, long timeout) throws Throwable {
		await(publisher, timeout, TimeUnit.SECONDS, true);
	}

	/**
	 * Wait {code timeout} in {@code unit} until a terminal signal from the passed publisher has been emitted.
	 * If the terminal signal is an error, it will propagate to the caller.
	 * Effectively this is making sure a stream has completed before the return of this call.
	 * It is usually used in controlled environment such as tests.
	 *
	 * @param publisher the publisher to listen for terminal signals
	 * @param timeout   the maximum wait time in unit
	 * @param unit      the TimeUnit to use for the timeout
	 */
	public static void await(Publisher<?> publisher, long timeout, TimeUnit unit) throws Throwable {
		await(publisher, timeout, unit, true);
	}

	/**
	 * Wait {code timeout} in {@code unit} until a terminal signal from the passed publisher has been emitted.
	 * If the terminal signal is an error, it will propagate to the caller.
	 * Effectively this is making sure a stream has completed before the return of this call.
	 * It is usually used in controlled environment such as tests.
	 *
	 * @param publisher the publisher to listen for terminal signals
	 * @param timeout   the maximum wait time in unit
	 * @param unit      the TimeUnit to use for the timeout
	 */
	public static void await(Publisher<?> publisher, long timeout, TimeUnit unit, final boolean request) throws
			Throwable {
		final AtomicReference<Throwable> exception = new AtomicReference<>();

		final CountDownLatch latch = new CountDownLatch(1);
		publisher.subscribe(new DefaultSubscriber<Object>() {
			Subscription s;

			@Override
			public void onSubscribe(Subscription subscription) {
				s = subscription;
				if (request) {
					subscription.request(Long.MAX_VALUE);
				}
			}

			@Override
			public void onError(Throwable throwable) {
				exception.set(throwable);
				cancel();
				latch.countDown();
			}

			@Override
			public void onComplete() {
				cancel();
				latch.countDown();
			}

			void cancel() {
				if (s != null) {
					try {
						s.cancel();
					} catch (Throwable t) {
						exception.set(t);
					}

				}
			}
		});

		latch.await(timeout, unit);
		if (exception.get() != null) {
			throw exception.get();
		}
	}

	private static final Stream NEVER = new Stream() {
		final Subscription NEVER_SUBSCRIPTION = new Subscription() {
			@Override
			public void request(long l) {
				//IGNORE
			}

			@Override
			public void cancel() {
				//IGNORE
			}
		};

		@Override
		public void subscribe(Subscriber subscriber) {
			if (subscriber != null) {
				subscriber.onSubscribe(NEVER_SUBSCRIPTION);
			}
		}
	};
}
