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
import reactor.core.publisher.FluxNever;
import reactor.core.publisher.MonoEmpty;
import reactor.core.publisher.MonoError;
import reactor.fn.Consumer;
import reactor.fn.Function;

/**
 * A Reactive Streams {@link Publisher} with basic rx operators that completes successfully
 * by emitting an element, or with an error.
 *
 * <p>{@code Mono<Void>} should be used for {Publisher} that just completes without any value.
 *
 * <p>It is intended to be used in implementation and return types, input parameters
 * should keep using raw {@link Publisher} as much as possible.
 *
 * TODO Implement methods with reactive-streams-commons, without using Publishers
 * TODO check the publisher emits only one element with {@code PublisherSingle} from reactive-streams-commons
 *
 * @author Sebastien Deleuze
 * @since 2.5
 * @see Flux
 */
public abstract class Mono<T> implements Publisher<T> {

	private static final Mono<?> EMPTY = new MonoEmpty(); // TODO



	/**
	 * Create a {@link Mono} that completes without emitting any item.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Mono<T> empty() {
		return (Mono<T>)EMPTY;
	}

	/**
	 * Create a {@link Mono} that completes with the specified error.
	 */
	public static <T> Mono<T> error(Throwable error) {
		return MonoError.<T>create(error);
	}

	/**
	 * Expose the specified {@link Publisher} with the {@link Mono} API, and ensure it
	 * will emit 0 or 1 item.
	 */
	public static <T> Mono<T> wrap(Publisher<T> source) {
		return Flux.wrap(source).first();
	}

	/**
	 * Create a new {@link Mono} that emits the specified item.
	 */
	public static <T> Mono<T> just(T data) {
		throw new UnsupportedOperationException(); // TODO
	}


	/**
	 * Return a {@code Mono<Void>} that completes when this {@link Mono} completes.
	 */
	public Mono<Void> after() {
		return flux().after();
	}

	/**
	 * Triggered when the {@link Mono} is unsubscribed.
	 */
	public Mono<T> doOnCancel(Runnable action) {
		return flux().doOnCancel(action).first();
	}

	/**
	 * Triggered when the {@link Mono} completes successfully.
	 */
	public Mono<T> doOnComplete(Consumer<? super T> action) {
		return flux().doOnNext(action).first();
	}

	/**
	 * Triggered when the {@link Mono} completes with an error.
	 */
	public Mono<T> doOnError(Consumer<Throwable> action) {
		return flux().doOnError(action).first();
	}

	/**
	 * Triggered when the {@link Mono} is subscribed.
	 */
	public Mono<T> doOnSubscribe(Runnable action) {
		return flux().doOnSubscribe(action).first();
	}

	/**
	 * Triggered when the {@link Mono} terminates, either by completing successfully or
	 * with an error.
	 */
	public Mono<T> doOnTerminate(Runnable action) {
		return flux().doOnTerminate(action).first();
	}

	/**
	 * Transform the items emitted by a {@link Publisher} into Publishers, then
	 * flatten the emissions from those by merging them into a single {@link Flux},
	 * so that they may interleave.
	 */
	public <R> Flux<R> flatMap(Function<? super T, ? extends Publisher<? extends R>> mapper) {
		return flux().flatMap(mapper);
	}

	/**
	 * Convert this {@link Mono} to a {@link Flux}
	 */
	public Flux<T> flux() {
		return new Flux.FluxBarrier<T, T>(this);
	}

	/**
	 * Transform the item emitted by this {@link Mono} by applying a function to item emitted.
	 */
	public <R> Mono<R> map(Function<? super T, ? extends R> mapper) {
		return flux().map(mapper).first();
	}

	/**
	 * Merge emissions of this {@link Mono} with the provided {@link Publisher}.
	 * TODO Varargs ?
	 */
	public Flux<T> mergeWith(Publisher<? extends T> source) {
		return flux().mergeWith(source);
	}

	/**
	 * Convert the value of {@link Mono} to another {@link Mono} possibly with another value type.
	 */
	public <R> Mono<R> then(Function<? super T, ? extends Mono<? extends R>> transformer) {
		return flux().flatMap(transformer).first();
	}

}
