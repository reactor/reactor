/*
 * Copyright (c) 2011-2016 Pivotal Software, Inc.
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

package reactor;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Level;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.processor.ProcessorGroup;
import reactor.core.publisher.FluxAmb;
import reactor.core.publisher.FluxFlatMap;
import reactor.core.publisher.FluxLift;
import reactor.core.publisher.FluxLog;
import reactor.core.publisher.FluxMap;
import reactor.core.publisher.FluxMapSignal;
import reactor.core.publisher.FluxPeek;
import reactor.core.publisher.FluxResume;
import reactor.core.publisher.FluxZip;
import reactor.core.publisher.MonoCallable;
import reactor.core.publisher.MonoEmpty;
import reactor.core.publisher.MonoError;
import reactor.core.publisher.MonoIgnoreElements;
import reactor.core.publisher.MonoJust;
import reactor.core.publisher.MonoNext;
import reactor.core.publisher.convert.DependencyUtils;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.subscription.CancelledSubscription;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.core.support.SignalType;
import reactor.core.support.internal.PlatformDependent;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.fn.tuple.Tuple2;
import reactor.fn.tuple.Tuple3;
import reactor.fn.tuple.Tuple4;
import reactor.fn.tuple.Tuple5;
import reactor.fn.tuple.Tuple6;

/**
 * A Reactive Streams {@link Publisher} with basic rx operators that completes successfully by emitting an element, or
 * with an error.
 *
 * <p>{@code Mono<Void>} should be used for {Publisher} that just completes without any value.
 *
 * <p>It is intended to be used in implementations and return types, input parameters should keep using raw {@link
 * Publisher} as much as possible.
 *
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 * @see Flux
 * @since 2.5
 */
public abstract class Mono<T> implements Publisher<T>, ReactiveState.Bounded {

	/**
	 * ==============================================================================================================
	 * ==============================================================================================================
	 * <p>
	 * Static Generators
	 * <p>
	 * ==============================================================================================================
	 * ==============================================================================================================
	 */

	/**
	 * Pick the first result coming from any of the given monos and populate a new {@literal Mono}.
	 *
	 * @param monos The deferred monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	@SafeVarargs
	@SuppressWarnings("varargs")
	public static <T> Mono<T> any(Mono<? extends T>... monos) {
		return new MonoBarrier<>(new FluxAmb<>(monos));
	}

	/**
	 * Pick the first result coming from any of the given monos and populate a new {@literal Mono}.
	 *
	 * @param monos The monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	public static <T> Mono<T> any(Iterable<? extends Mono<? extends T>> monos) {
		return new MonoBarrier<>(new FluxAmb<>(monos));
	}


	/**
	 * @param source
	 * @param <IN>
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <IN> Mono<IN> convert(Object source) {

		if (Publisher.class.isAssignableFrom(source.getClass())) {
			return from((Publisher<IN>) source);
		}
		else {
			return (Mono<IN>) Mono.from(DependencyUtils.convertToPublisher(source));
		}
	}

	/**
	 * Create a {@link Mono} that completes without emitting any item.
	 *
	 * @param <T>
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> Mono<T> empty() {
		return (Mono<T>) MonoEmpty.instance();
	}

	/**
	 * Create a {@link Mono} that completes with the specified error.
	 *
	 * @param error
	 * @param <T>
	 *
	 * @return
	 */
	public static <T> Mono<T> error(Throwable error) {
		return new MonoError<T>(error);
	}

	/**
	 * Expose the specified {@link Publisher} with the {@link Mono} API, and ensure it will emit 0 or 1 item.
	 *
	 * @param source
	 * @param <T>
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> Mono<T> from(Publisher<T> source) {
		if (source == null) {
			return empty();
		}
		if (Mono.class.isAssignableFrom(source.getClass())) {
			return (Mono<T>) source;
		}
		return new MonoNext<>(source);
	}

	/**
	 * Create a {@link Mono} producing the value for the {@link Mono} using the given supplier.
	 *
	 * @param supplier {@link Supplier} that will produce the value
	 * @param <T> type of the expected value
	 *
	 * @return A {@link Mono}.
	 */
	public static <T> Mono<T> fromCallable(Callable<? extends T> supplier) {
		return new MonoCallable<>(supplier);
	}

	/**
	 * Create a new {@link Mono} that emits the specified item.
	 *
	 * @param data
	 * @param <T>
	 *
	 * @return
	 */
	public static <T> Mono<T> just(T data) {
		return new MonoJust<>(data);
	}

	/**
	 * Aggregate given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono
	 * Monos} have been fulfilled.
	 *
	 * @param monos The monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	@SafeVarargs
	@SuppressWarnings({"varargs", "unchecked"})
	private static <T> Mono<List<T>> when(Mono<T>... monos) {
		return new MonoBarrier<>(new FluxZip<>(monos, FluxZip.TUPLE_TO_LIST_FUNCTION, 1));
	}

	/**
	 * Aggregate given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono
	 * Monos} have been fulfilled.
	 *
	 * @param monos The monos to use.
	 * @param <T> The type of the function result.
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Mono<List<T>> when(final Iterable<? extends Mono<? extends T>> monos) {
		return new MonoBarrier<>(new FluxZip<>(monos, FluxZip.TUPLE_TO_LIST_FUNCTION, 1));
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * @param p1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2> Mono<Tuple2<T1, T2>> when(Mono<T1> p1, Mono<T2> p2) {
		return new MonoBarrier<>(new FluxZip<>(new Mono[]{p1, p2}, Flux.IDENTITY_FUNCTION, 1));
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * @param p1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3> Mono<Tuple3<T1, T2, T3>> when(Mono<T1> p1, Mono<T2> p2, Mono<T3> p3) {
		return new MonoBarrier<>(new FluxZip<>(new Mono[]{p1, p2, p3}, Flux.IDENTITY_FUNCTION, 1));
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * @param p1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4> Mono<Tuple4<T1, T2, T3, T4>> when(Mono<T1> p1,
			Mono<T2> p2,
			Mono<T3> p3,
			Mono<T4> p4) {
		return new MonoBarrier<>(new FluxZip<>(new Mono[]{p1, p2, p3, p4}, Flux.IDENTITY_FUNCTION, 1));
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * @param p1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p5 The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5> Mono<Tuple5<T1, T2, T3, T4, T5>> when(Mono<T1> p1,
			Mono<T2> p2,
			Mono<T3> p3,
			Mono<T4> p4,
			Mono<T5> p5) {
		return new MonoBarrier<>(new FluxZip<>(new Mono[]{p1, p2, p3, p4, p5}, Flux.IDENTITY_FUNCTION, 1));
	}

	/**
	 * Merge given monos into a new a {@literal Mono} that will be fulfilled when all of the given {@literal Mono Monos}
	 * have been fulfilled.
	 *
	 * @param p1 The first upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p2 The second upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p3 The third upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p4 The fourth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p5 The fifth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param p6 The sixth upstream {@link org.reactivestreams.Publisher} to subscribe to.
	 * @param <T1> type of the value from source1
	 * @param <T2> type of the value from source2
	 * @param <T3> type of the value from source3
	 * @param <T4> type of the value from source4
	 * @param <T5> type of the value from source5
	 * @param <T6> type of the value from source6
	 *
	 * @return a {@link Mono}.
	 */
	@SuppressWarnings("unchecked")
	public static <T1, T2, T3, T4, T5, T6> Mono<Tuple6<T1, T2, T3, T4, T5, T6>> when(Mono<T1> p1,
			Mono<T2> p2,
			Mono<T3> p3,
			Mono<T4> p4,
			Mono<T5> p5,
			Mono<T6> p6) {
		return new MonoBarrier<>(new FluxZip<>(new Mono[]{p1, p2, p3, p4, p5, p6}, Flux.IDENTITY_FUNCTION, 1));
	}

	/**
	 * ==============================================================================================================
	 * ==============================================================================================================
	 * <p>
	 * Operators
	 * <p>
	 * ==============================================================================================================
	 * ==============================================================================================================
	 */
	protected Mono() {

	}

	/**
	 * Combine the result from this mono and another into a {@link Tuple2}.
	 *
	 * @param other
	 *
	 * @return a new combined Mono
	 * @see #when
	 */
	public final <T2> Mono<Tuple2<T, T2>> and(Mono<T2> other) {
		return when(this, other);
	}

	/**
	 * Return a {@code Mono<Void>} that completes when this {@link Mono} completes.
	 *
	 * @return
	 */
	public final Mono<Void> after() {
		return new MonoIgnoreElements<>(this);
	}

	/**
	 * Return a {@code Mono<Void>} that completes when this {@link Mono} completes.
	 *
	 * @return
	 */
	public final <V> Mono<V> after(Supplier<? extends Mono<V>> sourceSupplier) {
		return new MonoBarrier<>(after().flatMap(null, null, sourceSupplier));
	}

	/**
	 * Introspect this Mono graph
	 *
	 * @return {@link reactor.core.support.ReactiveStateUtils.Graph} representation of a publisher graph
	 */
	public final ReactiveStateUtils.Graph debug() {
		return ReactiveStateUtils.scan(this);
	}

	/**
	 * {@code mono.dispatchOn(Processors.queue()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param group
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public final Mono<T> dispatchOn(ProcessorGroup group) {
		return new MonoProcessorGroup<>(this, false, ((ProcessorGroup<T>) group));
	}

	/**
	 * Triggered when the {@link Mono} is cancelled.
	 *
	 * @param onCancel
	 *
	 * @return
	 */
	public final Mono<T> doOnCancel(Runnable onCancel) {
		return new MonoBarrier<>(new FluxPeek<>(this, null, null, null, null, null, null, onCancel));
	}

	/**
	 * Triggered when the {@link Mono} completes successfully.
	 *
	 * @param onSuccess
	 *
	 * @return
	 */
	public final Mono<T> doOnSuccess(Consumer<? super T> onSuccess) {
		return new MonoSuccess<>(this, onSuccess);
	}

	/**
	 * Triggered when the {@link Mono} completes with an error.
	 *
	 * @param onError
	 *
	 * @return
	 */
	public final Mono<T> doOnError(Consumer<? super Throwable> onError) {
		return new MonoBarrier<>(new FluxPeek<>(this, null, null, onError, null, null, null, null));
	}

	/**
	 * Triggered when the {@link Mono} is subscribed.
	 *
	 * @param onSubscribe
	 *
	 * @return
	 */
	public final Mono<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
		return new MonoBarrier<>(new FluxPeek<>(this, onSubscribe, null, null, null, null, null, null));
	}

	/**
	 * Triggered when the {@link Mono} terminates, either by completing successfully or with an error.
	 *
	 * @param onTerminate
	 *
	 * @return
	 */
	public final Mono<T> doOnTerminate(BiConsumer<? super T, Throwable> onTerminate) {
		return new MonoSuccess<>(this, onTerminate);
	}

	/**
	 * Transform the items emitted by a {@link Publisher} into Publishers, then flatten the emissions from those by
	 * merging them into a single {@link Flux}, so that they may interleave.
	 *
	 * @param mapper
	 * @param <R>
	 *
	 * @return
	 */
	public final <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return new FluxFlatMap<>(this, mapper, ReactiveState.SMALL_BUFFER_SIZE, Integer.MAX_VALUE);
	}

	/**
	 * Transform the signals emitted by this {@link Flux} into Publishers, then flatten the emissions from those by
	 * merging them into a single {@link Flux}, so that they may interleave.
	 *
	 * @param mapperOnNext
	 * @param mapperOnError
	 * @param mapperOnComplete
	 * @param <R>
	 *
	 * @return
	 *
	 * @see Flux#flatMap(Function, Function, Supplier)
	 */
	@SuppressWarnings("unchecked")
	public final <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapperOnNext,
			Function<Throwable, ? extends Publisher<? extends R>> mapperOnError,
			Supplier<? extends Publisher<? extends R>> mapperOnComplete) {
		return new FluxFlatMap<>(
				new FluxMapSignal<>(this, mapperOnNext, mapperOnError, mapperOnComplete),
				Flux.IDENTITY_FUNCTION,
				ReactiveState.SMALL_BUFFER_SIZE, 32);
	}

	/**
	 * Convert this {@link Mono} to a {@link Flux}
	 *
	 * @return
	 */
	public final Flux<T> flux() {
		return new Flux.FluxBarrier<T, T>(this);
	}

	/**
	 * Block until a next signal is received, will return null if onComplete, T if onNext, throw a
	 * ReactorFatalException if checked error or origin RuntimeException if unchecked.
	 * If the default timeout {@link #DEFAULT_TIMEOUT} has elapsed, a CancelException will be thrown.
	 *
	 * @return T the result
	 */
	public T get() {
		return get(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
	}

	/**
	 * Block until a next signal is received, will return null if onComplete, T if onNext, throw a
	 * ReactorFatalException if checked error or origin RuntimeException if unchecked.
	 * If the default timeout {@link #DEFAULT_TIMEOUT} has elapsed, a CancelException will be thrown.
	 *
	 * Note that each get() will subscribe a new single (MonoResult) subscriber, in other words, the result might
	 * miss signal from hot publishers.
	 *
	 * @param timeout
	 * @param unit
	 *
	 * @return T the result
	 */
	public T get(long timeout, TimeUnit unit) {
		MonoResult<T> result = new MonoResult<>();
		subscribe(result);
		return result.await(timeout, unit);
	}

	/**
	 * Create a {@link Mono} intercepting all source signals with the returned Subscriber that might choose to pass them
	 * alone to the provided Subscriber (given to the returned {@code subscribe(Subscriber)}.

	 * @param lifter
	 * @param <V>
	 * @return
	 *
	 * @see Flux#lift
	 */
	public final <V> Mono<V> lift(Function<Subscriber<? super V>, Subscriber<? super T>> lifter) {
		return new FluxLift.MonoLift<>(this, lifter);
	}

	/**
	 * @return
	 *
	 * @see Flux#log()
	 */
	public final Mono<T> log() {
		return log(null, Level.INFO, FluxLog.ALL);
	}

	/**
	 * @param category
	 *
	 * @return
	 */
	public final Mono<T> log(String category) {
		return log(category, Level.INFO, FluxLog.ALL);
	}

	/**
	 * @param category
	 * @param level
	 *
	 * @return
	 */
	public final Mono<T> log(String category, Level level) {
		return log(category, level, FluxLog.ALL);
	}

	/**
	 * @param category
	 * @param level
	 * @param options
	 *
	 * @return
	 */
	public final Mono<T> log(String category, Level level, int options) {
		return new MonoBarrier<>(new FluxLog<>(this, category, level, options));
	}

	/**
	 * Transform the item emitted by this {@link Mono} by applying a function to item emitted.
	 *
	 * @param mapper
	 * @param <R>
	 *
	 * @return
	 */
	public final <R> Mono<R> map(Function<? super T, ? extends R> mapper) {
		return new MonoBarrier<>(new FluxMap<>(this, mapper));
	}

	/**
	 * Merge emissions of this {@link Mono} with the provided {@link Publisher}.
	 *
	 * @param source
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public final Flux<T> mergeWith(Publisher<? extends T> source) {
		return Flux.merge(Flux.just(this, source));
	}

	/**
	 * Emit the any of the result from this mono or from the given mono
	 *
	 * @param source
	 *
	 * @return a new Mono
	 * @see #any
	 */
	public final Mono<T> or(Mono<? extends T> source) {
		return any(this, source);
	}

	/**
	 * Subscribe to a returned fallback publisher when any error occurs.
	 *
	 * @param fallback
	 *
	 * @return
	 * @see Flux#onErrorResumeWith
	 */
	public final Mono<T> otherwise(Function<Throwable, ? extends Mono<? extends T>> fallback) {
		return new MonoBarrier<>(new FluxResume<>(this, fallback));
	}

	/**
	 * Run the requests to this Publisher {@link Mono} on a given processor thread from the given {@link
	 * ProcessorGroup}
	 * <p>
	 * {@code mono.publishOn(Processors.ioGroup()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param group
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public final Mono<T> publishOn(ProcessorGroup group) {
		return new MonoProcessorGroup<>(this, true, ((ProcessorGroup<T>) group));
	}

	/**
	 * Start the chain and request unbounded demand.
	 */
	public final void subscribe() {
		subscribe(Subscribers.unbounded());
	}

	/**
	 * Convert the value of {@link Mono} to another {@link Mono} possibly with another value type.
	 *
	 * @param transformer
	 * @param <R>
	 *
	 * @return
	 */
	public final <R> Mono<R> then(Function<? super T, ? extends Mono<? extends R>> transformer) {
		return new MonoBarrier<>(flatMap(transformer));
	}

	/**
	 * Subscribe the {@link Mono} with the givne {@link Subscriber} and return it.
	 *
	 * @param subscriber
	 * @param <E>
	 *
	 * @return
	 */
	public final <E extends Subscriber<? super T>> E to(E subscriber) {
		subscribe(subscriber);
		return subscriber;
	}

	@Override
	public final long getCapacity() {
		return 1L;
	}

	/**
	 * ==============================================================================================================
	 * ==============================================================================================================
	 *
	 * Containers
	 *
	 * ==============================================================================================================
	 * ==============================================================================================================
	 */

	/**
	 * A connecting Mono Publisher (right-to-left from a composition chain perspective)
	 *
	 * @param <I>
	 * @param <O>
	 */
	public static class MonoBarrier<I, O> extends Mono<O> implements Factory, Named, Upstream {

		protected final Publisher<? extends I> source;

		public MonoBarrier(Publisher<? extends I> source) {
			this.source = source;
		}

		@Override
		public String getName() {
			return getClass().getSimpleName().replaceAll("Mono|Stream|Operator", "");
		}

		/**
		 * Default is delegating and decorating with Mono API
		 *
		 * @param s
		 */
		@Override
		@SuppressWarnings("unchecked")
		public void subscribe(Subscriber<? super O> s) {
			source.subscribe((Subscriber<? super I>) s);
		}

		@Override
		public String toString() {
			return "{" +
					" operator : \"" + getName() + "\" " +
					'}';
		}

		@Override
		public final Publisher<? extends I> upstream() {
			return source;
		}
	}

	static final class MonoProcessorGroup<I> extends MonoBarrier<I, I> implements FeedbackLoop {

		private final ProcessorGroup<I> processor;
		private final boolean publishOn;

		public MonoProcessorGroup(Publisher<? extends I> source, boolean publishOn, ProcessorGroup<I> processor) {
			super(source);
			this.publishOn = publishOn;
			this.processor = processor;
		}

		@Override
		public void subscribe(Subscriber<? super I> s) {
			if(publishOn) {
				processor.publishOn(source)
				         .subscribe(s);
			}
			else{
				processor.dispatchOn(source)
				         .subscribe(s);
			}
		}

		@Override
		public Object delegateInput() {
			return processor;
		}

		@Override
		public Object delegateOutput() {
			return processor;
		}
	}

	static final class MonoSuccess<I> extends MonoBarrier<I, I> implements FeedbackLoop{

		private final Consumer<? super I>   onSuccess;
		private final BiConsumer<? super I, Throwable> onSuccessOrFailure;

		MonoSuccess(Publisher<? extends I> source, Consumer<? super I> onSuccess) {
			super(source);
			this.onSuccess = Objects.requireNonNull(onSuccess);
			this.onSuccessOrFailure = null;
		}

		MonoSuccess(Publisher<? extends I> source, BiConsumer<? super I, Throwable> onSuccessOrFailure) {
			super(source);
			this.onSuccess = null;
			this.onSuccessOrFailure = Objects.requireNonNull(onSuccessOrFailure);
		}

		@Override
		public void subscribe(Subscriber<? super I> s) {
			if(onSuccessOrFailure != null) {
				source.subscribe(new MonoSuccessBarrier<>(s, onSuccessOrFailure));
			}
			else{
				source.subscribe(new MonoSuccessBarrier<>(s, onSuccess));
			}
		}

		@Override
		public Object delegateInput() {
			return onSuccess;
		}

		@Override
		public Object delegateOutput() {
			return null;
		}

		private static final class MonoSuccessBarrier<I> extends SubscriberBarrier<I, I> {
			private final Consumer<? super I> onSuccess;
			private final BiConsumer<? super I, Throwable> onSuccessOrFailure;

			public MonoSuccessBarrier(Subscriber<? super I> s, BiConsumer<? super I, Throwable> onSuccessOrFailure) {
				super(s);
				this.onSuccess = null;
				this.onSuccessOrFailure = onSuccessOrFailure;
			}

			public MonoSuccessBarrier(Subscriber<? super I> s, Consumer<? super I> onSuccess) {
				super(s);
				this.onSuccess = onSuccess;
				this.onSuccessOrFailure = null;
			}

			@Override
			protected void doComplete() {
				if(upstream() == null){
					return;
				}
				if(onSuccess != null){
					onSuccess.accept(null);
				}
				else{
					onSuccessOrFailure.accept(null, null);
				}
				subscriber.onComplete();
			}

			@Override
			protected void doNext(I t) {
				if(upstream() == null){
					Exceptions.onNextDropped(t);
					return;
				}
				cancel();
				if(onSuccess != null) {
					onSuccess.accept(t);
				}
				else{
					onSuccessOrFailure.accept(t, null);
				}
				subscriber.onNext(t);
				subscriber.onComplete();
			}

			@Override
			protected void doError(Throwable throwable) {
				if(onSuccessOrFailure != null) {
					onSuccessOrFailure.accept(null, throwable);
				}
				subscriber.onError(throwable);
			}
		}
	}

	final static class MonoResult<I> implements Subscriber<I>, ActiveUpstream {

		volatile SignalType   endState;
		volatile I            value;
		volatile Throwable    error;
		volatile Subscription s;

		static final AtomicReferenceFieldUpdater<MonoResult, Subscription> SUBSCRIPTION =
				PlatformDependent.newAtomicReferenceFieldUpdater(MonoResult.class, "s");

		public I await(long timeout, TimeUnit unit) {
			long delay = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, unit);

			try {
				for (; ; ) {
					SignalType endState = this.endState;
					if(endState != null) {
						switch (endState) {
							case NEXT:
								return value;
							case ERROR:
								if (error instanceof RuntimeException) {
									throw (RuntimeException) error;
								}
								throw ReactorFatalException.create(error);
							case COMPLETE:
								return null;
						}
					}
					if(delay < System.currentTimeMillis()){
						throw CancelException.get();
					}
					Thread.sleep(1);
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw ReactorFatalException.create(e);
			}
			finally {
				Subscription s = SUBSCRIPTION.getAndSet(this, CancelledSubscription.INSTANCE);

				if (s != null && s != CancelledSubscription.INSTANCE) {
					s.cancel();
				}
			}
		}

		@Override
		public boolean isStarted() {
			return s != null && endState == null;
		}

		@Override
		public boolean isTerminated() {
			return endState != null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(I i) {
			s.cancel();
			if (endState != null) {
				Exceptions.onNextDropped(i);
			}
			value = i;
			endState = SignalType.NEXT;
		}

		@Override
		public void onError(Throwable t) {
			if (endState != null) {
				Exceptions.onErrorDropped(t);
			}
			error = t;
			endState = SignalType.ERROR;
		}

		@Override
		public void onComplete() {
			if (endState != null) {
				return;
			}
			endState = SignalType.COMPLETE;
		}
	}
}
