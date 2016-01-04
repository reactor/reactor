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
import java.util.concurrent.Callable;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.processor.ProcessorGroup;
import reactor.core.publisher.FluxFlatMap;
import reactor.core.publisher.FluxMap;
import reactor.core.publisher.FluxPeek;
import reactor.core.publisher.FluxZip;
import reactor.core.publisher.MonoCallable;
import reactor.core.publisher.MonoEmpty;
import reactor.core.publisher.MonoError;
import reactor.core.publisher.MonoFirst;
import reactor.core.publisher.MonoIgnoreElements;
import reactor.core.publisher.MonoJust;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
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
 * <p>
 * <p>{@code Mono<Void>} should be used for {Publisher} that just completes without any value.
 * <p>
 * <p>It is intended to be used in implementation and return types, input parameters should keep using raw {@link
 * Publisher} as much as possible.
 *
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 * @see Flux
 * @since 2.5
 */
public abstract class Mono<T> implements Publisher<T> {

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
		return Flux.amb(monos)
		           .first();
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
		return Flux.amb(monos)
		           .first();
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
	 * Create a {@link Mono} producing the value for the {@link Mono} using the given supplier.
	 *
	 * @param supplier {@link Supplier} that will produce the value
	 * @param <T> type of the expected value
	 *
	 * @return A {@link Mono}.
	 */
	public static <T> Mono<T> from(Callable<? extends T> supplier) {
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
		return new FluxZip<>(monos, FluxZip.TUPLE_TO_LIST_FUNCTION, 1).first();
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
		return new FluxZip<>(monos, FluxZip.TUPLE_TO_LIST_FUNCTION, 1).first();
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
		return new FluxZip<>(new Mono[]{p1, p2}, Flux.IDENTITY_FUNCTION, 1).first();
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
		return new FluxZip<>(new Mono[]{p1, p2, p3}, Flux.IDENTITY_FUNCTION, 1).first();
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
		return new FluxZip<>(new Mono[]{p1, p2, p3, p4}, Flux.IDENTITY_FUNCTION, 1).first();
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
		return new FluxZip<>(new Mono[]{p1, p2, p3, p4, p5}, Flux.IDENTITY_FUNCTION, 1).first();
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
		return new FluxZip<>(new Mono[]{p1, p2, p3, p4, p5, p6}, Flux.IDENTITY_FUNCTION, 1).first();
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
	public static <T> Mono<T> wrap(Publisher<T> source) {
		if (source == null) {
			return empty();
		}
		if (Mono.class.isAssignableFrom(source.getClass())) {
			return (Mono<T>) source;
		}
		return new MonoFirst<>(source);
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
	 * Return a {@code Mono<Void>} that completes when this {@link Mono} completes.
	 *
	 * @return
	 */
	public final Mono<Void> after() {
		return new MonoIgnoreElements<>(this);
	}

	/**
	 * Call {@link #subscribe(Subscriber)} and return the passed {@link Subscriber}, allows for chaining, e.g. :
	 * <p>
	 * {@code Processors.topic().process(Processors.queue()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param group
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public final Mono<T> dispatchOn(ProcessorGroup group) {
		return new MonoBarrier<>(((ProcessorGroup<T>) group).dispatchOn());
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
	 * @param onComplete
	 *
	 * @return
	 */
	public final Mono<T> doOnComplete(Runnable onComplete) {
		return new MonoBarrier<>(new FluxPeek<>(this, null, null, null, onComplete, null, null, null));
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
	public final Mono<T> doOnTerminate(Runnable onTerminate) {
		return new MonoBarrier<>(new FluxPeek<>(this, null, null, null, null, onTerminate, null, null));
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
	 * Convert this {@link Mono} to a {@link Flux}
	 *
	 * @return
	 */
	public final Flux<T> flux() {
		return new Flux.FluxBarrier<T, T>(this);
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
	 * Call {@link #subscribe(Subscriber)} and return the passed {@link Subscriber}, allows for chaining, e.g. :
	 * <p>
	 * {@code Processors.topic().process(Processors.queue()).subscribe(Subscribers.unbounded()) }
	 *
	 * @param group
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public final Mono<T> publishOn(ProcessorGroup group) {
		return new MonoBarrier<>(((ProcessorGroup<T>) group).publishOn());
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
	public static class MonoBarrier<I, O> extends Mono<O>
			implements ReactiveState.Factory, ReactiveState.Bounded, ReactiveState.Named, ReactiveState.Upstream {

		protected final Publisher<? extends I> source;

		public MonoBarrier(Publisher<? extends I> source) {
			this.source = source;
		}

		@Override
		public long getCapacity() {
			return 1L;
		}

		@Override
		public String getName() {
			return ReactiveStateUtils.getName(getClass().getSimpleName())
			                         .replaceAll("Mono|Stream|Operator", "");
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

}
