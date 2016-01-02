/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Flux;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Function;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
public class FluxLift<I, O> extends Flux.FluxBarrier<I, O> implements Flux.Operator<I, O> {

	/**
	 * Intercept a source {@link Publisher} onNext signal to eventually transform, forward or filter the data by calling
	 * or not the right operand {@link Subscriber}.
	 *
	 * @param dataConsumer A {@link BiConsumer} with left argument onNext data and right argument output subscriber
	 * @param <I> The source type of the data sequence
	 * @param <O> The target type of the data sequence
	 *
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Flux<O> lift(Publisher<I> source,
			BiConsumer<I, SubscriberWithContext<? super O, Subscription>> dataConsumer) {
		return lift(source, dataConsumer, null, null);
	}

	/**
	 * Intercept a source {@link Publisher} onNext signal to eventually transform, forward or filter the data by calling
	 * or not the right operand {@link Subscriber}.
	 *
	 * @param dataConsumer A {@link BiConsumer} with left argument onNext data and right argument output subscriber
	 * @param errorConsumer A {@link BiConsumer} with left argument onError throwable and right argument output sub
	 * @param <I> The source type of the data sequence
	 * @param <O> The target type of the data sequence
	 *
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Flux<O> lift(Publisher<I> source,
			BiConsumer<I, SubscriberWithContext<? super O, Subscription>> dataConsumer,
			BiConsumer<Throwable, Subscriber<? super O>> errorConsumer) {
		return lift(source, dataConsumer, errorConsumer, null);
	}

	/**
	 * Intercept a source {@link Publisher} onNext signal to eventually transform, forward or filter the data by calling
	 * or not the right operand {@link Subscriber}. <p> The argument {@code subscriptionHandler} is executed once by new
	 * subscriber to generate a context shared by every request calls.
	 *
	 * @param dataConsumer A {@link BiConsumer} with left argument onNext data and right argument output subscriber
	 * @param errorConsumer A {@link BiConsumer} with left argument onError throwable and right argument output sub
	 * @param completeConsumer A {@link Consumer} called onComplete with the actual output subscriber
	 * @param <I> The source type of the data sequence
	 * @param <O> The target type of the data sequence
	 *
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Flux<O> lift(Publisher<I> source,
			final BiConsumer<I, SubscriberWithContext<? super O, Subscription>> dataConsumer,
			final BiConsumer<Throwable, Subscriber<? super O>> errorConsumer,
			final Consumer<Subscriber<? super O>> completeConsumer) {
		return lift(source, new Function<Subscriber<? super O>, Subscriber<? super I>>() {
			@Override
			public SubscriberBarrier<I, O> apply(final Subscriber<? super O> subscriber) {
				return new ConsumerSubscriberBarrier<>(subscriber, dataConsumer, errorConsumer, completeConsumer);
			}
		});
	}

	/**
	 * Create a {@link Publisher} intercepting all source signals with the returned Subscriber that might choose to pass
	 * them alone to the provided Subscriber (given to the returned {@link Publisher#subscribe(Subscriber)}.
	 *
	 * @param source A {@link Publisher} source delegate
	 * @param barrierProvider A {@link Function} called once for every new subscriber returning a unique {@link
	 * Subscriber} to intercept upstream signals
	 * @param <I> The type of the data sequence
	 * @param <O> The type of contextual information to be read by the requestConsumer
	 *
	 * @return a fresh Reactive Streams publisher ready to be subscribed
	 */
	public static <I, O> Flux<O> lift(Publisher<I> source,
			Function<Subscriber<? super O>, Subscriber<? super I>> barrierProvider) {
		return new FluxLift<>(source, barrierProvider);
	}

	/**
	 * @param left
	 * @param right
	 * @param <I>
	 * @param <O>
	 * @param <E>
	 *
	 * @return
	 */
	public static <I, O, E> Function<Subscriber<? super I>, Subscriber<? super O>> opFusion(final Function<Subscriber<? super I>, Subscriber<? super E>> left,
			final Function<Subscriber<? super E>, Subscriber<? super O>> right) {
		return new Function<Subscriber<? super I>, Subscriber<? super O>>() {
			@Override
			public Subscriber<? super O> apply(Subscriber<? super I> subscriber) {
				return right.apply(left.apply(subscriber));
			}
		};
	}

	final private Function<Subscriber<? super O>, Subscriber<? super I>> barrierProvider;

	public FluxLift(Publisher<I> source, Function<Subscriber<? super O>, Subscriber<? super I>> barrierProvider) {
		super(source);
		this.barrierProvider = barrierProvider;
	}

	@Override
	public void subscribe(Subscriber<? super O> s) {
		source.subscribe(apply(s));
	}

	@Override
	public Subscriber<? super I> apply(Subscriber<? super O> subscriber) {
		return barrierProvider.apply(subscriber);
	}

	private static final class ConsumerSubscriberBarrier<I, O> extends SubscriberBarrier<I, O> {

		private final BiConsumer<I, SubscriberWithContext<? super O, Subscription>> dataConsumer;
		private final BiConsumer<Throwable, Subscriber<? super O>>                  errorConsumer;
		private final Consumer<Subscriber<? super O>>                               completeConsumer;

		private SubscriberWithContext<? super O, Subscription> subscriberWithContext;

		public ConsumerSubscriberBarrier(Subscriber<? super O> subscriber,
				BiConsumer<I, SubscriberWithContext<? super O, Subscription>> dataConsumer,
				BiConsumer<Throwable, Subscriber<? super O>> errorConsumer,
				Consumer<Subscriber<? super O>> completeConsumer) {
			super(subscriber);
			this.dataConsumer = dataConsumer;
			this.errorConsumer = errorConsumer;
			this.completeConsumer = completeConsumer;
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			subscriberWithContext = SubscriberWithContext.create(subscriber, subscription);
			subscriber.onSubscribe(subscription);
		}

		@Override
		protected void doNext(I o) {
			if (dataConsumer != null) {
				dataConsumer.accept(o, subscriberWithContext);
			}
			else {
				super.doNext(o);
			}
		}

		@Override
		protected void doError(Throwable throwable) {
			if (errorConsumer != null) {
				errorConsumer.accept(throwable, subscriber);
			}
			else {
				super.doError(throwable);
			}
		}

		@Override
		protected void doComplete() {
			if (completeConsumer != null) {
				completeConsumer.accept(subscriber);
			}
			else {
				super.doComplete();
			}
		}

		@Override
		public String toString() {
			return "ConsumerSubscriberBarrier{" +
					"dataConsumer=" + dataConsumer +
					", errorConsumer=" + errorConsumer +
					", completeConsumer=" + completeConsumer +
					'}';
		}
	}
}
