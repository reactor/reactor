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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.processor.BaseProcessor;
import reactor.core.publisher.FluxArray;
import reactor.core.publisher.FluxFlatMap;
import reactor.core.publisher.FluxJust;
import reactor.core.publisher.FluxLift;
import reactor.core.publisher.FluxMap;
import reactor.core.publisher.FluxNever;
import reactor.core.publisher.FluxPeek;
import reactor.core.publisher.FluxSession;
import reactor.core.publisher.FluxZip;
import reactor.core.publisher.MonoIgnoreElements;
import reactor.core.publisher.MonoSingle;
import reactor.core.subscription.ReactiveSession;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.fn.BiFunction;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.fn.tuple.Tuple2;

/**
 * A Reactive Streams {@link Publisher} with basic rx operators that emits 0 to N elements, and then complete
 * (successfully or with an error).
 * <p>
 * <p>It is intended to be used in Reactive Spring projects implementation and return types. Input parameters should
 * keep using raw {@link Publisher} as much as possible.
 * <p>
 * <p>If it is known that the underlying {@link Publisher} will emit 0 or 1 element, {@link Mono} should be used
 * instead.
 * <p>
 * TODO Implement methods with reactive-streams-commons, without using Publishers
 *
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 * @see Mono
 * @since 2.5
 */
public abstract class Flux<T> implements Publisher<T>, ReactiveState {

	private static final Flux<?> EMPTY = Mono.empty()
	                                         .flux();

	/**
	 *
	 *  Static Generators
	 *
	 */

	/**
	 * Create a {@link Flux} that completes without emitting any item.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Flux<T> empty() {
		return (Flux<T>) EMPTY;
	}

	/**
	 * Create a {@link Flux} that completes with the specified error.
	 */
	public static <T> Flux<T> error(Throwable error) {
		return Mono.<T>error(error).flux();
	}

	/**
	 *
	 * @param source
	 * @param <T>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> Flux<T> merge(Publisher<? extends Publisher<? extends T>> source) {
		return new FluxFlatMap<>(source, BaseProcessor.XS_BUFFER_SIZE, Integer.MAX_VALUE);
	}

	/**
	 * Expose the specified {@link Publisher} with the {@link Flux} API.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Flux<T> wrap(Publisher<T> source) {
		if (Flux.class.isAssignableFrom(source.getClass())) {
			return (Flux<T>) source;
		}

		if (Supplier.class.isAssignableFrom(source.getClass())) {
			T t = ((Supplier<T>)source).get();
			if(t != null){
				return just(t);
			}
		}
		return new FluxBarrier<>(source);
	}

	/**
	 * Create a {@link Flux} that emits the items contained in the provided {@link Iterable}.
	 */
	public static <T> Flux<T> from(Iterable<? extends T> it) {
		throw new UnsupportedOperationException(); // TODO
	}


	/**
	 * Create a {@link Flux} that emits the items contained in the provided {@link Iterable}.
	 */
	public static <T> Flux<T> from(T[] array) {
		return new FluxArray<>(array);
	}


	/**
	 * Create a {@link Flux} reacting on subscribe with the passed {@link Consumer}. The argument {@code
	 * sessionConsumer} is executed once by new subscriber to generate a {@link ReactiveSession} context ready to accept
	 * signals.
	 * @param sessionConsumer A {@link Consumer} called once everytime a subscriber subscribes
	 * @param <T> The type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <T> Flux<T> yield(Consumer<? super ReactiveSession<T>> sessionConsumer) {
		return new FluxSession<>(sessionConsumer);
	}


	/**
	 * Create a new {@link Flux} that emits the specified item.
	 */
	@SafeVarargs
	public static <T> Flux<T> just(T... data) {
		return from(data);
	}

	public static <T> Flux<T> just(T data) {
		return new FluxJust<>(data);
	}

	/**
	 * Create a {@link Flux} that never completes.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Flux<T> never() {
		return FluxNever.instance();
	}

	/**
	 * Intercept a source {@link Publisher} onNext signal to eventually transform, forward or filter the data by calling
	 * or not the right operand {@link Subscriber}.
	 * @param transformer A {@link Function} that transforms each emitting sequence item
	 * @param <I> The source type of the data sequence
	 * @param <O> The target type of the data sequence
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Flux<O> map(Publisher<I> source, final Function<? super I, ? extends O> transformer) {
		return new FluxMap<>(source, transformer);
	}

	/**
	 *
	 *  Instance Operators
	 *
	 *
	 */
	protected Flux() {
	}

	/**
	 * Return a {@code Mono<Void>} that completes when this {@link Flux} completes.
	 */
	public Mono<Void> after() {
		return new MonoIgnoreElements<>(this);
	}

	/**
	 *
	 * @param capacity
	 * @return
	 */
	public  Flux<T> capacity(long capacity) {
		return new Flux.FluxBounded<>(this, capacity);
	}

	/**
	 * Like {@link #flatMap(Function)}, but concatenate emissions instead of merging (no interleave).
	 */
	public <R> Flux<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Concatenate emissions of this {@link Flux} with the provided {@link Publisher} (no interleave). TODO Varargs ?
	 */
	public Flux<T> concatWith(Publisher<? extends T> source) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Triggered when the {@link Flux} is unsubscribed.
	 */
	public Flux<T> doOnCancel(Runnable onCancel) {
		return new FluxPeek<>(this, null, null, null, null, null, null, onCancel);
	}

	/**
	 * Triggered when the {@link Flux} completes successfully.
	 */
	public Flux<T> doOnComplete(Runnable onComplete) {
		return new FluxPeek<>(this, null, null, null, onComplete, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} completes with an error.
	 */
	public Flux<T> doOnError(Consumer<? super Throwable> onError) {
		return new FluxPeek<>(this, null, null, onError, null, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} emits an item.
	 */
	public Flux<T> doOnNext(Consumer<? super T> onNext) {
		return new FluxPeek<>(this, null, onNext, null, null, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} is subscribed.
	 */
	public Flux<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
		return new FluxPeek<>(this, onSubscribe, null, null, null, null, null, null);
	}

	/**
	 * Triggered when the {@link Flux} terminates, either by completing successfully or with an error.
	 */
	public Flux<T> doOnTerminate(Runnable onTerminate) {
		return new FluxPeek<>(this, null, null, null, null, onTerminate, null, null);
	}

	/**
	 * Emit only the first item emitted by this {@link Flux}.
	 */
	public Mono<T> first() {
		return new MonoSingle<>(this);
	}

	/**
	 * Transform the items emitted by this {@link Flux} into Publishers, then flatten the emissions from those by
	 * merging them into a single {@link Flux}, so that they may interleave.
	 */
	public <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return new FluxFlatMap<>(this, mapper, BaseProcessor.SMALL_BUFFER_SIZE, 32);
	}

	/**
	 * Create a {@link Flux} intercepting all source signals with the returned Subscriber that might choose to pass them
	 * alone to the provided Subscriber (given to the returned {@code subscribe(Subscriber)}.
	 */
	public <R> Flux<R> lift(Function<Subscriber<? super R>, Subscriber<? super T>> operator) {
		return new FluxLift<>(this, operator);
	}

	/**
	 * Transform the items emitted by this {@link Flux} by applying a function to each item.
	 */
	public <R> Flux<R> map(Function<? super T, ? extends R> mapper) {
		return new FluxMap<>(this, mapper);
	}

	/**
	 * Merge emissions of this {@link Flux} with the provided {@link Publisher}, so that they may interleave.
	 */
	public Flux<T> mergeWith(Publisher<? extends T> source) {
		return merge(just(this, source));
	}

	/**
	 *
	 * @return
	 */
	public Flux<T> unbounded() {
		return capacity(Long.MAX_VALUE);
	}

	/**
	 * Combine the emissions of multiple Publishers together via a specified function and emit single items for each
	 * combination based on the results of this function.
	 */
	public <R, V> Flux<V> zipWith(Publisher<? extends R> source2,
			final BiFunction<? super T, ? super R, ? extends V> zipper) {

		return new FluxZip<>(new Publisher[]{this, source2}, new Function<Tuple2<T, R>, V>() {
			@Override
			public V apply(Tuple2<T, R> tuple) {
				return zipper.apply(tuple.getT1(), tuple.getT2());
			}
		}, BaseProcessor.XS_BUFFER_SIZE);

	}

	/**
	 * A marker interface for components responsible for augmenting subscribers with features like {@link
	 * #lift}
	 */
	public interface Operator<I, O>
			extends Function<Subscriber<? super O>, Subscriber<? super I>>, Factory {

	}

	/**
	 * A connecting Flux Publisher (right-to-left from a composition chain perspective)
	 *
	 * @param <I>
	 * @param <O>
	 */
	public static class FluxBarrier<I, O> extends Flux<O> implements Factory, Bounded, Named, Upstream {

		protected final Publisher<? extends I> source;

		public FluxBarrier(Publisher<? extends I> source) {
			this.source = source;
		}

		@Override
		public long getCapacity() {
			return Bounded.class.isAssignableFrom(source.getClass()) ? ((Bounded) source).getCapacity() :
					Long.MAX_VALUE;
		}

		@Override
		public String getName() {
			return ReactiveStateUtils.getName(getClass().getSimpleName())
			                         .replaceAll("Flux|Stream|Operator", "");
		}

		/**
		 * Default is delegating and decorating with Flux API
		 * @param s
		 */
		@Override
		@SuppressWarnings("unchecked")
		public void subscribe(Subscriber<? super O> s) {
			source.subscribe((Subscriber<? super I>)s);
		}

		@Override
		public final Publisher<? extends I> upstream() {
			return source;
		}

		@Override
		public String toString() {
			return "{" +
					" operator : \"" + getName() + "\" " +
					'}';
		}
	}

	/**
	 * Decorate a Flux with a capacity for downstream accessors
	 *
	 * @param <I>
	 */
	private final static class FluxBounded<I> extends FluxBarrier<I, I> {

		final private long         capacity;

		public FluxBounded(Publisher<I> source, long capacity) {
			super(source);
			this.capacity = capacity;
		}

		@Override
		public long getCapacity() {
			return capacity;
		}

		@Override
		public String getName() {
			return "Bounded";
		}

		@Override
		public void subscribe(Subscriber<? super I> s) {
			source.subscribe(s);
		}
	}
}
