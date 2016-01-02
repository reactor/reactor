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
import reactor.core.publisher.FluxMap;
import reactor.core.publisher.FluxPeek;
import reactor.core.publisher.MonoEmpty;
import reactor.core.publisher.MonoError;
import reactor.core.publisher.MonoIgnoreElements;
import reactor.core.publisher.MonoJust;
import reactor.core.publisher.MonoSingle;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
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
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 *
 * @since 2.5
 * @see Flux
 */
public abstract class Mono<T> implements Publisher<T> {

	/**
	 * Create a {@link Mono} that completes without emitting any item.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Mono<T> empty() {
		return (Mono<T>)MonoEmpty.instance();
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
		return new MonoSingle<>(source);
	}

	/**
	 * Create a new {@link Mono} that emits the specified item.
	 */
	public static <T> Mono<T> just(T data) {
		return new MonoJust<>(data);
	}


	/**
	 * Return a {@code Mono<Void>} that completes when this {@link Mono} completes.
	 */
	public final Mono<Void> after() {
		return new MonoIgnoreElements<>(this);
	}

	/**
	 * Triggered when the {@link Mono} is unsubscribed.
	 */
	public Mono<T> doOnCancel(Runnable onCancel) {
		return new MonoBarrier<>(new FluxPeek<>(this, null, null, null, null, null, null, onCancel));
	}

	/**
	 * Triggered when the {@link Mono} completes successfully.
	 */
	public Mono<T> doOnComplete(Runnable onComplete) {
		return new MonoBarrier<>(new FluxPeek<>(this, null, null, null, onComplete, null, null, null));
	}

	/**
	 * Triggered when the {@link Mono} completes with an error.
	 */
	public Mono<T> doOnError(Consumer<? super Throwable> onError) {
		return new MonoBarrier<>(new FluxPeek<>(this, null, null, onError, null, null, null, null));
	}

	/**
	 * Triggered when the {@link Mono} is subscribed.
	 */
	public Mono<T> doOnSubscribe(Consumer<? super Subscription> onSubscribe) {
		return new MonoBarrier<>(new FluxPeek<>(this, onSubscribe, null, null, null, null, null, null));
	}

	/**
	 * Triggered when the {@link Mono} terminates, either by completing successfully or
	 * with an error.
	 */
	public Mono<T> doOnTerminate(Runnable onTerminate) {
		return new MonoBarrier<>(new FluxPeek<>(this, null, null, null, null, onTerminate, null, null));
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
	public final Flux<T> flux() {
		return new Flux.FluxBarrier<T, T>(this);
	}

	/**
	 * Transform the item emitted by this {@link Mono} by applying a function to item emitted.
	 */
	public <R> Mono<R> map(Function<? super T, ? extends R> mapper) {
		return new MonoBarrier<>(new FluxMap<>(this, mapper));
	}

	/**
	 * Merge emissions of this {@link Mono} with the provided {@link Publisher}.
	 */
	@SuppressWarnings("unchecked")
	public Flux<T> mergeWith(Publisher<? extends T> source) {
		return Flux.merge(Flux.just(this, source));
	}

	/**
	 * Convert the value of {@link Mono} to another {@link Mono} possibly with another value type.
	 */
	public final <R> Mono<R> then(Function<? super T, ? extends Mono<? extends R>> transformer) {
		return new MonoBarrier<>(flatMap(transformer));
	}

	/**
	 * A connecting Mono Publisher (right-to-left from a composition chain perspective)
	 *
	 * @param <I>
	 * @param <O>
	 */
	public static class MonoBarrier<I, O> extends Mono<O> implements ReactiveState.Factory, ReactiveState.Bounded,
	                                                                 ReactiveState.Named, ReactiveState.Upstream {

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
}
