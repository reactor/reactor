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
import reactor.fn.BiFunction;
import reactor.fn.Consumer;
import reactor.fn.Function;

/**
 * A Reactive Streams {@link Publisher} with basic rx operators that emits 0 to N elements,
 * and then complete (successfully or with an error).
 *
 * <p>It is intended to be used in Reactive Spring projects implementation and return types.
 * Input parameters should keep using raw {@link Publisher} as much as possible.
 *
 * <p>If it is known that the underlying {@link Publisher} will emit 0 or 1 element,
 * {@link Mono} should be used instead.
 *
 * TODO Implement methods with reactive-streams-commons, without using Publishers
 *
 * @author Sebastien Deleuze
 * @since 2.5
 * @see Mono
 */
public abstract class Flux<T> implements Publisher<T> {


	/**
	 * Create a {@link Flux} that completes without emitting any item.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Flux<T> empty() {
		return Mono.<T>empty().flux();
	}

	/**
	 * Create a {@link Flux} that completes with the specified error.
	 */
	public static <T> Flux<T> error(Throwable error) {
		return Mono.<T>error(error).flux();
	}

	/**
	 * Expose the specified {@link Publisher} with the {@link Flux} API.
	 * TODO Optimize when the Publisher is a Flux
	 */
	public static <T> Flux<T> wrap(Publisher<T> source) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Create a {@link Flux} that emits the items contained in the provided {@link Iterable}.
	 */
	public static <T> Flux<T> from(Iterable<? extends T> it) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Create a {@link Flux} reacting on each available {@link Subscriber} read derived with the
	 * passed {@link Consumer}. If a previous request is still running, avoid recursion and extend
	 * the previous request iterations.
	 */
	public static <T> Flux<T> generate(Consumer<Subscriber<? super T>> requestConsumer) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Create a new {@link Flux} that emits the specified item.
	 */
	public static <T> Flux<T> just(T data) {
		return Mono.<T>just(data).flux();
	}

	/**
	 * Create a {@link Flux} that never completes.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Flux<T> never() {
		return Mono.<T>never().flux();
	}


	/**
	 * Return a {@code Mono<Void>} that completes when this {@link Flux} completes.
	 */
	public Mono<Void> after() {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Concatenate emissions of this {@link Flux} with the provided {@link Publisher}
	 * (no interleave).
	 * TODO Varargs ?
	 */
	public Flux<T> concatWith(Publisher<? extends T> source) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Like {@link #flatMap(Function)}, but concatenate emissions instead of merging
	 * (no interleave).
	 */
	public <R> Flux<R> concatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Triggered when the {@link Flux} is unsubscribed.
	 */
	public Flux<T> doOnCancel(Runnable action) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Triggered when the {@link Flux} completes successfully.
	 */
	public Flux<T> doOnComplete(Runnable action) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Triggered when the {@link Flux} completes with an error.
	 */
	public Flux<T> doOnError(Consumer<Throwable> action) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Triggered when the {@link Flux} emits an item.
	 */
	public Flux<T> doOnNext(Consumer<? super T> action) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Triggered when the {@link Flux} is subscribed.
	 */
	public Flux<T> doOnSubscribe(Runnable action) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Triggered when the {@link Flux} terminates, either by completing successfully or
	 * with an error.
	 */
	public Flux<T> doOnTerminate(Runnable action) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Emit only the first item emitted by this {@link Flux}.
	 */
	public Mono<T> first() {
		return Mono.wrap(this);
	}

	/**
	 * Transform the items emitted by this {@link Flux} into Publishers, then
	 * flatten the emissions from those by merging them into a single {@link Flux},
	 * so that they may interleave.
	 */
	public <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Create a {@link Flux} intercepting all source signals with the returned Subscriber that might choose to pass
	 * them alone to the provided Subscriber (given to the returned {@code subscribe(Subscriber)}.
	 */
	public <R> Flux<R> lift(Function<Subscriber<? super R>, Subscriber<? super T>> operator) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Transform the items emitted by this {@link Flux} by applying a function to each item.
	 */
	public <R> Flux<R> map(Function<? super T, ? extends R> mapper) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Merge emissions of this {@link Flux} with the provided {@link Publisher}, so that
	 * they may interleave.
	 * TODO Varargs ?
	 */
	public Flux<T> mergeWith(Publisher<? extends T> source) {
		throw new UnsupportedOperationException(); // TODO
	}

	/**
	 * Combine the emissions of multiple Publishers together via a specified function and
	 * emit single items for each combination based on the results of this function.
	 */
	public <R, V> Flux<V> zip(Publisher<? extends R> source, BiFunction<? super T, ? super R, ? extends V> zipper) {
		throw new UnsupportedOperationException(); // TODO
	}


	// TODO Use FluxBarrier to make the bridge with reactive-streams-commons
	public static abstract class FluxBarrier<I, O> extends Flux<O> {

		protected final Publisher<I> source;

		public FluxBarrier(Publisher<I> source) {
			this.source = source;
		}

	}

}
