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

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Flux;
import reactor.Mono;
import reactor.Processors;
import reactor.Timers;
import reactor.core.error.Exceptions;
import reactor.core.publisher.FluxZip;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.core.subscription.EmptySubscription;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.ReactiveState;
import reactor.core.support.rb.disruptor.RingBuffer;
import reactor.core.timer.Timer;
import reactor.fn.BiConsumer;
import reactor.fn.BiFunction;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.fn.tuple.Tuple3;
import reactor.fn.tuple.Tuple4;
import reactor.fn.tuple.Tuple5;
import reactor.fn.tuple.Tuple6;
import reactor.fn.tuple.Tuple7;
import reactor.rx.broadcast.StreamProcessor;
import reactor.rx.stream.StreamBarrier;
import reactor.rx.stream.StreamCombineLatest;
import reactor.rx.stream.StreamConcatArray;
import reactor.rx.stream.StreamConcatIterable;
import reactor.rx.stream.StreamDefer;
import reactor.rx.stream.StreamFuture;
import reactor.rx.stream.StreamIterable;
import reactor.rx.stream.StreamJust;
import reactor.rx.stream.StreamRange;
import reactor.rx.stream.StreamSwitchMap;
import reactor.rx.stream.StreamTimerPeriod;
import reactor.rx.stream.StreamTimerSingle;
import reactor.rx.stream.StreamWithLatestFrom;

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
 * Stream.yield( subscriber -> {
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

	/**
	 * @see Flux#yield(Consumer)
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> yield(Consumer<? super ReactiveSession<T>> sessionConsumer) {
		return from(Flux.yield(sessionConsumer));
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
		if (requestConsumer == null) throw new IllegalArgumentException("Supplier must be provided");
		return createWith(requestConsumer, null, null);
	}

	/**
	 * Create a {@link Stream} reacting on requests with the passed {@link BiConsumer}
	 * The argument {@code contextFactory} is executed once by new subscriber to generate a context shared by every
	 * request calls.
	 *
	 * @param requestConsumer A {@link BiConsumer} with left argument request and right argument target subscriber
	 * @param contextFactory  A {@link Function} called for every new subscriber returning an immutable context (IO
	 *                        connection...)
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
	 *                         (IO connection...)
	 * @param shutdownConsumer A {@link Consumer} called once everytime a subscriber terminates: cancel, onComplete(),
	 *                         onError()
	 * @param <T>              The type of the data sequence
	 * @param <C>              The type of contextual information to be read by the requestConsumer
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 * @since 2.0.2
	 */
	public static <T, C> Stream<T> createWith(BiConsumer<Long, SubscriberWithContext<T, C>> requestConsumer,
	                                          Function<Subscriber<? super T>, C> contextFactory,
	                                          Consumer<C> shutdownConsumer) {
		return Streams.from(Flux.generate(requestConsumer, contextFactory, shutdownConsumer));
	}

	/**
	 * @see Flux#create(Consumer)
	 */
	public static <T> Stream<T> create(Consumer<SubscriberWithContext<T, Void>> request) {
		return from(Flux.create(request));
	}

	/**
	 * @see Flux#create(Consumer, Function)
	 */
	public static <T, C> Stream<T> create(Consumer<SubscriberWithContext<T, C>> request,
			Function<Subscriber<? super T>, C> onSubscribe) {
		return from(Flux.create(request, onSubscribe));
	}

	/**
	 * @see Flux#create(Consumer, Function, Consumer)
	 */
	public static <T, C> Stream<T> create(Consumer<SubscriberWithContext<T, C>> request,
			Function<Subscriber<? super T>, C> onSubscribe,
			Consumer<C> onTerminate) {
		return from(Flux.create(request, onSubscribe, onTerminate));
	}

	/**
	 * A simple decoration of the given {@link Publisher} to expose {@link Stream} API and proxy any subscribe call to
	 * the publisher.
	 * The Publisher has to first call onSubscribe and receive a subscription request callback before any onNext
	 * call or
	 * will risk loosing events.
	 *
	 * @param publisher the publisher to decorate the Stream subscriber
	 * @param <T>       the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stream<T> from(final Publisher<T> publisher) {
		if (Stream.class.isAssignableFrom(publisher.getClass())) {
			return (Stream<T>) publisher;
		}

		if (Supplier.class.isAssignableFrom(publisher.getClass())) {
			T t = ((Supplier<T>)publisher).get();
			if(t != null){
				return just(t);
			}
		}
		return new StreamBarrier.Identity<>(publisher);
	}

	/**
	 * A simple decoration of the given {@link Processor} to expose {@link Stream} API and proxy any subscribe call to
	 * the Processor.
	 * The Processor has to first call onSubscribe and receive a subscription request callback before any onNext
	 * call or
	 * will risk loosing events.
	 *
	 * @param processor the processor to decorate with the Stream API
	 * @param <I>       the type of values observed by the receiving subscriber
	 * @param <O>       the type of values passing through the sending {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	@SuppressWarnings("unchecked")
	public static <I, O> StreamProcessor<I, O> from(final Processor<I, O> processor) {
		if (StreamProcessor.class.isAssignableFrom(processor.getClass())) {
			return (StreamProcessor<I, O>) processor;
		}
		return StreamProcessor.from(processor);
	}

	/**
	 *
	 * @param publisher
	 * @param operator
	 * @param <I>
	 * @param <O>
	 * @return
	 */
	public static <I, O> Stream<O> lift(final Publisher<I> publisher,
			Function<Subscriber<? super O>, Subscriber<? super I>> operator) {
		return new StreamBarrier.Operator<>(publisher, operator);
	}

	/**
	 * Supply a {@link Publisher} everytime subscribe is called on the returned stream. The passed {@link reactor.fn
	 * .Supplier}
	 * will be invoked and it's up to the developer to choose to return a new instance of a {@link Publisher} or reuse
	 * one,
	 * effecitvely behaving like {@link reactor.rx.Streams#from(Publisher)}.
	 *
	 * @param supplier the publisher factory to call on subscribe
	 * @param <T>      the type of values passing through the {@literal Stream}
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> defer(Supplier<? extends Publisher<T>> supplier) {
		return new StreamDefer<>(supplier);
	}

	/**
	 * Build a {@literal Stream} that will only emit a complete signal to any new subscriber.
	 *
	 * @return a new {@link Stream}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stream<T> empty() {
		return (Stream<T>) StreamJust.EMPTY;
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
		return from(Mono.<O>error(throwable));
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
	public static <T> Stream<T> fromIterable(Iterable<? extends T> values) {
		return new StreamIterable<>(values);
	}

	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterator on subscription request.
	 * <p>
	 * It will use the passed dispatcher to emit signals.
	 *
	 * @param values The values to {@code onNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	public static <T> Stream<T> fromIterator(Iterator<? extends T> values) {
		return from(Flux.fromIterator(values));
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
	public static <T> Stream<T> fromArray(T[] values) {
		return from(Flux.fromArray(values));
	}

	/**
	 * Build a {@literal Stream} that will only emit the result of the future and then complete.
	 * The future will be polled for an unbounded amount of time.
	 *
	 * @param future the future to poll value from
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> fromFuture(Future<? extends T> future) {
		return StreamFuture.create(future);
	}

	/**
	 * Build a {@literal Stream} that will only emit the result of the future and then complete.
	 * The future will be polled for an unbounded amount of time.
	 *
	 * @param future the future to poll value from
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static <T> Stream<T> fromFuture(Future<? extends T> future, long time, TimeUnit unit) {
		return StreamFuture.create(future, time, unit);
	}

	/**
	 * Build a {@literal Stream} that will only emit a sequence of int within the specified range and then
	 * complete.
	 *
	 * @param start the starting value to be emitted
	 * @param count   the number ot times to emit an increment including the first value
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Integer> range(int start, int count) {
		return new StreamRange(start, count);
	}

	/**
	 * Build a {@literal Stream} that will only emit 0l after the time delay and then complete.
	 *
	 * @param delay the timespan in SECONDS to wait before emitting 0l and complete signals
	 * @return a new {@link reactor.rx.Stream}
	 */
	public static Stream<Long> timer(long delay) {
		return timer(Timers.globalOrNew(), delay, TimeUnit.SECONDS);
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
		return timer(Timers.globalOrNew(), delay, unit);
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
		return new StreamTimerSingle(delay, unit, timer);
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
		return period(Timers.globalOrNew(), -1l, period, TimeUnit.SECONDS);
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
		return period(Timers.globalOrNew(), delay, period, TimeUnit.SECONDS);
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
		return period(Timers.globalOrNew(), -1l, period, unit);
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
		return period(Timers.globalOrNew(), delay, period, unit);
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
		return new StreamTimerPeriod(TimeUnit.MILLISECONDS.convert(delay, unit), period, unit, timer);
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
		if(value1 == null){
			throw new Exceptions.Spec213_ArgumentIsNull();
		}

		return new StreamJust<T>(value1);
	}

	/**
	 * Build a {@literal Stream} whom data is sourced by each element of the passed iterable on subscription
	 * request.
	 * <p>
	 *
	 * @param values The values to {@code onNext()}
	 * @param <T>    type of the values
	 * @return a {@link Stream} based on the given values
	 */
	@SafeVarargs
	@SuppressWarnings({"unchecked", "varargs"})
	public static <T> Stream<T> just(T... values) {
		return from(Flux.fromArray(Objects.requireNonNull(values)));
	}


	/**
	 * @see Flux#convert(Object)
	 * @since 2.5
	 */
	public static <T> Stream<T> convert(Object source) {
		return from(Flux.<T>convert(source));
	}

	/**
	 * Build a Synchronous {@literal Action} whose data are emitted by the most recent {@link Subscriber#onNext(Object)}
	 * signaled publisher.
	 * The stream will complete once both the publishers source and the last switched to publisher have completed.
	 *
	 * @param <T> type of the value
	 * @return a {@link StreamProcessor} accepting publishers and producing inner data T
	 * @since 2.0
	 */
	public static <T> StreamProcessor<Publisher<? extends T>, T> switchOnNext() {
		Processor<Publisher<? extends T>, Publisher<? extends T>> emitter = Processors.replay();
		StreamProcessor<Publisher<? extends T>, T> p = StreamProcessor.from(emitter, switchOnNext(emitter));
		p.onSubscribe(EmptySubscription.INSTANCE);
		return p;
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
	@SuppressWarnings("unchecked")
	public static <T> Stream<T> switchOnNext(
	  Publisher<Publisher<? extends T>> mergedPublishers) {
		return new StreamSwitchMap<>(mergedPublishers, IDENTITY_FUNCTION, XS_QUEUE_SUPPLIER, ReactiveState.XS_BUFFER_SIZE);
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.

	 *
	 * @param mergedPublishers The list of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>              type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stream<T> amb(Iterable<? extends Publisher<? extends T>> mergedPublishers) {
		return from(Flux.amb(mergedPublishers));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

	 *
	 * @param sources The upstreams {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0, 2.5
	 */
	@SafeVarargs
	@SuppressWarnings({"unchecked", "varargs"})
	public static <T> Stream<T> amb(Publisher<? extends T>... sources) {
		return from(Flux.amb(sources));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been
	 * passed
	 * to.
	 *
	 * @param mergedPublishers The list of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>              type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stream<T> concat(Iterable<? extends Publisher<? extends T>> mergedPublishers) {
		return new StreamConcatIterable<>(mergedPublishers);
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been
	 * passed
	 * to.
	 *
	 * @param concatdPublishers The publisher of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>               type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T> Stream<T> concat(Publisher<? extends Publisher<? extends T>> concatdPublishers) {
		return from(Flux.concat(concatdPublishers));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.
	 * Each source publisher will be consumed until complete in sequence, with the same order than they have been
	 * passed
	 * to.
	 *
	 * @param sources The upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0, 2.5
	 */
	@SuppressWarnings("varargs")
	@SafeVarargs
	public static <T> Stream<T> concat(Publisher<? extends T>... sources) {
		return new StreamConcatArray<>(sources);
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.

	 *
	 * @param mergedPublishers The list of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>              type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stream<T> merge(Iterable<? extends Publisher<? extends T>> mergedPublishers) {
		return from(Flux.merge(mergedPublishers));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are generated by the passed publishers.

	 *
	 * @param mergedPublishers The publisher of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>              type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <T, E extends T> Stream<E> merge(Publisher<? extends Publisher<E>> mergedPublishers) {
		return from(Flux.merge(mergedPublishers));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

	 *
	 * @param sources The upstreams {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0, 2.5
	 */
	@SafeVarargs
	@SuppressWarnings({"unchecked", "varargs"})
	public static <T> Stream<T> merge(Publisher<? extends T>... sources) {
		return from(Flux.merge(sources));
	}


	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.
	 *
	 * @param sources    The upstreams {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <T>       type of the value from sources
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0, 2.5
	 */
	@SuppressWarnings({"unchecked", "varargs"})
	@SafeVarargs
	public static <T, V> Stream<V> combineLatest(final Function<Object[], V> combinator,
			Publisher<? extends T>... sources) {
		if (sources == null || sources.length == 0) {
			return empty();
		}

		if (sources.length == 1) {
			return from((Publisher<V>) sources[0]);
		}

		return new StreamCombineLatest<>(sources,
				combinator,
				(Supplier<? extends Queue<StreamCombineLatest.SourceAndArray>>) XS_QUEUE_SUPPLIER,
				ReactiveState.XS_BUFFER_SIZE);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.
	 *
	 * @param source1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the value
	 * to signal downstream
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <V> The produced output after transformation by {@param combinator}
	 *
	 * @return a {@link Stream} based on the produced value
	 *
	 * @since 2.5
	 */
	public static <T1, T2, V> Stream<V> combineLatest(Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			BiFunction<? super T1, ? super T2, ? extends V> combinator) {
		return new StreamWithLatestFrom<>(source1, source2, combinator);
	}
	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.

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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, V> Stream<V> combineLatest(Publisher<? extends T1> source1,
	                                                      Publisher<? extends T2> source2,
	                                                      Publisher<? extends T3> source3,
			Function<Object[], V> combinator) {
		return combineLatest(combinator, source1, source2, source3);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.

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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, V> Stream<V> combineLatest(Publisher<? extends T1> source1,
	                                                          Publisher<? extends T2> source2,
	                                                          Publisher<? extends T3> source3,
	                                                          Publisher<? extends T4> source4,
			Function<Object[], V> combinator) {
		return combineLatest(combinator, source1, source2, source3, source4);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.

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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, V> Stream<V> combineLatest(Publisher<? extends T1> source1,
	                                                              Publisher<? extends T2> source2,
	                                                              Publisher<? extends T3> source3,
	                                                              Publisher<? extends T4> source4,
	                                                              Publisher<? extends T5> source5,
			Function<Object[], V> combinator) {
		return combineLatest(combinator, source1, source2, source3, source4, source5);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.

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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6, V> Stream<V> combineLatest(Publisher<? extends T1> source1,
	                                                                  Publisher<? extends T2> source2,
	                                                                  Publisher<? extends T3> source3,
	                                                                  Publisher<? extends T4> source4,
	                                                                  Publisher<? extends T5> source5,
	                                                                  Publisher<? extends T6> source6,
			Function<Object[], V> combinator) {
		return combineLatest(combinator, source1, source2, source3, source4, source5, source6);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.

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
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6, T7, V> Stream<V> combineLatest(Publisher<? extends T1> source1,
	                                                                      Publisher<? extends T2> source2,
	                                                                      Publisher<? extends T3> source3,
	                                                                      Publisher<? extends T4> source4,
	                                                                      Publisher<? extends T5> source5,
	                                                                      Publisher<? extends T6> source6,
	                                                                      Publisher<? extends T7> source7,
			Function<Object[], V> combinator) {
		return combineLatest(combinator, source1, source2, source3, source4, source5, source6, source7);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.

	 *
	 * @param sources    The list of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0, 2.5
	 */
	@SuppressWarnings("unchecked")
	public static <T, V> Stream<V> combineLatest(Iterable<? extends Publisher<? extends T>> sources,
			final Function<Object[], V> combinator) {
		if (sources == null) {
			return empty();
		}

		return new StreamCombineLatest<>(sources,
				combinator,
				(Supplier<? extends Queue<StreamCombineLatest.SourceAndArray>>) XS_QUEUE_SUPPLIER,
				ReactiveState.XS_BUFFER_SIZE
		);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the combination of the most recent published values from
	 * all publishers.

	 *
	 * @param sources    The publisher of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0, 2.5
	 */
	@SuppressWarnings("unchecked")
	public static <V> Stream<V> combineLatest(Publisher<? extends Publisher<?>> sources,
			final Function<Object[], V> combinator) {
		return from(sources).buffer()
		                    .flatMap(new Function<List<? extends Publisher<?>>, Publisher<V>>() {
					@Override
					public Publisher<V> apply(List<? extends Publisher<?>> publishers) {
						return new StreamCombineLatest<Object, V>(publishers,
								combinator,
								(Supplier<? extends Queue<StreamCombineLatest.SourceAndArray>>) XS_QUEUE_SUPPLIER,
								ReactiveState.XS_BUFFER_SIZE);
					}
		                    }
		);
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

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
	                                        BiFunction<? super T1, ? super T2, ? extends V> combinator) {
		return from(Flux.zip(source1, source2, combinator));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 *                   value to signal downstream
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2> Stream<Tuple2<T1, T2>> zip(Publisher<? extends T1> source1,
	                                                  Publisher<? extends T2> source2) {
		return from(Flux.zip(source1, source2));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

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
		return from(new FluxZip<>(new Publisher[]{source1, source2, source3},
				combinator,
				ReactiveState.XS_BUFFER_SIZE));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @return a {@link Stream} based on the produced value
	 * @since 2.5
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3> Stream<Tuple3<T1, T2, T3>> zip(Publisher<? extends T1> source1,
												              Publisher<? extends T2> source2,
												              Publisher<? extends T3> source3) {
		return from(new FluxZip<>(new Publisher[]{source1, source2, source3},
				(Function<Tuple3<T1, T2, T3>, Tuple3<T1, T2, T3>>) IDENTITY_FUNCTION,
				ReactiveState.XS_BUFFER_SIZE));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

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
		return from(new FluxZip<>(new Publisher[]{source1, source2, source3, source4},
				combinator,
				ReactiveState.XS_BUFFER_SIZE));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4    The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <T4>       type of the value from source4
	 * @return a {@link Stream} based on the produced value
	 * @since 2.5
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4> Stream<Tuple4<T1, T2, T3, T4>> zip(Publisher<? extends T1> source1,
													                  Publisher<? extends T2> source2,
													                  Publisher<? extends T3> source3,
													                  Publisher<? extends T4> source4) {
		return from(new FluxZip<>(new Publisher[]{source1, source2, source3, source4},
				IDENTITY_FUNCTION,
				ReactiveState.XS_BUFFER_SIZE));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

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
		return from(new FluxZip<>(new Publisher[]{source1, source2, source3, source4, source5},
				combinator,
				ReactiveState.XS_BUFFER_SIZE));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4    The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <T4>       type of the value from source4
	 * @param <T5>       type of the value from source5
	 * @return a {@link Stream} based on the produced value
	 * @since 2.5
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5> Stream<Tuple5<T1, T2, T3, T4, T5>> zip(Publisher<? extends T1> source1,
														                      Publisher<? extends T2> source2,
														                      Publisher<? extends T3> source3,
														                      Publisher<? extends T4> source4,
														                      Publisher<? extends T5> source5) {
		return from(new FluxZip<>(new Publisher[]{source1, source2, source3, source4, source5},
				(Function<Tuple5<T1, T2, T3, T4, T5>, Tuple5<T1, T2, T3, T4, T5>>) IDENTITY_FUNCTION,
				ReactiveState.XS_BUFFER_SIZE));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

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
		return from(new FluxZip<>(new Publisher[]{source1, source2, source3, source4, source5, source6},
				combinator,
				ReactiveState.XS_BUFFER_SIZE));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

	 *
	 * @param source1    The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source2    The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source3    The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source4    The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source5    The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param source6    The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T1>       type of the value from source1
	 * @param <T2>       type of the value from source2
	 * @param <T3>       type of the value from source3
	 * @param <T4>       type of the value from source4
	 * @param <T5>       type of the value from source5
	 * @param <T6>       type of the value from source6
	 * @return a {@link Stream} based on the produced value
	 * @since 2.5
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6> Stream<Tuple6<T1, T2, T3, T4, T5, T6>> zip(Publisher<? extends T1> source1,
															                          Publisher<? extends T2> source2,
															                          Publisher<? extends T3> source3,
															                          Publisher<? extends T4> source4,
															                          Publisher<? extends T5> source5,
															                          Publisher<? extends T6> source6) {
		return from(new FluxZip<>(new Publisher[]{source1, source2, source3, source4, source5, source6},
				(Function<Tuple6<T1, T2, T3, T4, T5, T6>, Tuple6<T1, T2, T3, T4, T5, T6>>) IDENTITY_FUNCTION,
				ReactiveState.XS_BUFFER_SIZE));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

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
		return from(new FluxZip<>(new Publisher[]{source1, source2, source3, source4, source5, source6, source7},
				combinator,
				ReactiveState.XS_BUFFER_SIZE));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

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
	 * @return a {@link Stream} based on the produced value
	 * @since 2.5
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6, T7> Stream<Tuple7<T1, T2, T3, T4, T5, T6, T7>> zip(
			Publisher<? extends T1> source1,
			Publisher<? extends T2> source2,
			Publisher<? extends T3> source3,
			Publisher<? extends T4> source4,
			Publisher<? extends T5> source5,
			Publisher<? extends T6> source6,
			Publisher<? extends T7> source7) {
		return from(new FluxZip<>(new Publisher[]{source1, source2, source3, source4, source5, source6, source7},
				IDENTITY_FUNCTION,
				ReactiveState.XS_BUFFER_SIZE));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

	 *
	 * @param sources    The list of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @param <TUPLE>    The type of tuple to use that must match source Publishers type
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <TUPLE extends Tuple, V> Stream<V> zip(Iterable<? extends Publisher<?>> sources,
			final Function<? super TUPLE, ? extends V> combinator) {
		return from(Flux.zip(sources, new Function<Tuple, V>() {
			@Override
			@SuppressWarnings("unchecked")
			public V apply(Tuple tuple) {
				return combinator.apply((TUPLE)tuple);
			}
		}));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

	 *
	 * @param sources    The list of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <TUPLE>    The type of tuple to use that must match source Publishers type
	 * @return a {@link Stream} based on the produced value
	 * @since 2.5
	 */
	@SuppressWarnings("unchecked")
	public static <TUPLE extends Tuple> Stream<TUPLE> zip(Iterable<? extends Publisher<?>> sources) {
		return from((Publisher<TUPLE>) Flux.zip(sources));
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

	 *
	 * @param sources    The publisher of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param combinator The aggregate function that will receive a unique value from each upstream and return the
	 *                   value to signal downstream
	 * @param <V>        The produced output after transformation by {@param combinator}
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	public static <TUPLE extends Tuple, V> Stream<V> zip(
	  Publisher<? extends Publisher<?>> sources,
	  final Function<? super TUPLE, ? extends V> combinator) {

		return from(sources).buffer()
		                    .flatMap(new Function<List<? extends Publisher<?>>, Publisher<V>>() {
			@Override
			public Publisher<V> apply(List<? extends Publisher<?>> publishers) {
				return new FluxZip<>(publishers.toArray(
						new Publisher[publishers.size()]),
						combinator,
						ReactiveState.XS_BUFFER_SIZE);
			}
		});
	}

	/**
	 * Build a {@literal Stream} whose data are generated by the passed publishers.

	 *
	 * @param sources    The publisher of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @return a {@link Stream} based on the produced value
	 * @since 2.5
	 */
	@SuppressWarnings("unchecked")
	public static Stream<Tuple> zip(Publisher<? extends Publisher<?>> sources) {
		return zip(sources, (Function<Tuple, Tuple>) IDENTITY_FUNCTION);
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are aggregated from the passed publishers
	 * (1 element consumed for each merged publisher. resulting in an array of size of {@param mergedPublishers}.

	 *
	 * @param sources The upstreams {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	@SuppressWarnings({"unchecked", "varargs"})
	@SafeVarargs
	public static <T> Stream<List<T>> join(Publisher<? extends T>... sources) {
		return from(Flux.zip(FluxZip.JOIN_FUNCTION, sources));
	}

	/**
	 * Build a Synchronous {@literal Stream} whose data are aggregated from the passed publishers
	 * (1 element consumed for each merged publisher. resulting in an array of size of {@param mergedPublishers}.

	 *
	 * @param sources The list of upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T>     type of the value
	 * @return a {@link Stream} based on the produced value
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public static <T> Stream<List<T>> join(Iterable<? extends Publisher<?>> sources) {
		return zip(sources, FluxZip.JOIN_FUNCTION);
	}

	/**
	 * Wait 30 Seconds until a terminal signal from the passed publisher has been emitted.
	 * If the terminal signal is an error, it will propagate to the caller.
	 * Effectively this is making sure a stream has completed before the return of this call.
	 * It is usually used in controlled environment such as tests.
	 *
	 * @param publisher the publisher to listen for terminal signals
	 */
	public static void await(Publisher<?> publisher) throws InterruptedException {
		await(publisher, 30000, TimeUnit.MILLISECONDS);
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
	public static void await(Publisher<?> publisher, long timeout, TimeUnit unit) throws InterruptedException {
		final AtomicReference<Throwable> exception = new AtomicReference<>();

		final CountDownLatch latch = new CountDownLatch(1);
		publisher.subscribe(new BaseSubscriber<Object>() {
			Subscription s;

			@Override
			public void onComplete() {
				s = null;
				latch.countDown();
			}

			@Override
			public void onError(Throwable throwable) {
				s = null;
				exception.set(throwable);
				latch.countDown();
			}

			@Override
			public void onSubscribe(Subscription subscription) {
				s = subscription;
				subscription.request(Long.MAX_VALUE);
			}
		});

		latch.await(timeout, unit);
		if (exception.get() != null) {
			InterruptedException ie = new InterruptedException();
			Exceptions.addCause(ie, exception.get());
			throw ie;
		}
	}

	static final Stream NEVER = from(Flux.never());

	static final Function IDENTITY_FUNCTION = new Function() {
		@Override
		public Object apply(Object o) {
			return o;
		}
	};

	static final Supplier XS_QUEUE_SUPPLIER = new Supplier() {
		@Override
		public Object get() {
			return RingBuffer.newSequencedQueue(RingBuffer.createSingleProducer(ReactiveState.XS_BUFFER_SIZE));
		}
	};

	protected Streams() {
	}
}
