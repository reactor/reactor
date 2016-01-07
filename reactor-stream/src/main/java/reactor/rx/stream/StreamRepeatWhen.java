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
package reactor.rx.stream;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import reactor.fn.Function;

import org.reactivestreams.*;

import reactor.core.processor.EmitterProcessor;
import reactor.core.subscriber.*;
import reactor.core.subscription.DeferredSubscription;
import reactor.core.subscription.EmptySubscription;

/**
 * Repeats a source when a companion sequence
 * signals an item in response to the main's completion signal
 * <p>
 * <p>If the companion sequence signals when the main source is active, the repeat
 * attempt is suppressed and any terminal signal will terminate the main source with the same signal immediately.
 *
 * @param <T> the source value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamRepeatWhen<T> extends StreamBarrier<T, T> {

	final Function<? super reactor.rx.Stream<Object>, ? extends Publisher<? extends Object>> whenSourceFactory;

	public StreamRepeatWhen(Publisher<? extends T> source,
							   Function<? super reactor.rx.Stream<Object>, ? extends Publisher<? extends Object>> whenSourceFactory) {
		super(source);
		this.whenSourceFactory = Objects.requireNonNull(whenSourceFactory, "whenSourceFactory");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {

		StreamRepeatWhenOtherSubscriber other = new StreamRepeatWhenOtherSubscriber();

		reactor.rx.subscriber.SerializedSubscriber<T> serial = new reactor.rx.subscriber.SerializedSubscriber<>(s);

		StreamRepeatWhenMainSubscriber<T> main = new StreamRepeatWhenMainSubscriber<>(serial, other
		  .completionSignal, source);
		other.main = main;

		serial.onSubscribe(main);

		Publisher<? extends Object> p;

		try {
			p = whenSourceFactory.apply(other);
		} catch (Throwable e) {
			s.onError(e);
			return;
		}

		if (p == null) {
			s.onError(new NullPointerException("The whenSourceFactory returned a null Publisher"));
			return;
		}

		p.subscribe(other);

		if (!main.cancelled) {
			source.subscribe(main);
		}
	}

	static final class StreamRepeatWhenMainSubscriber<T> extends SubscriberMultiSubscription<T, T> {

		final DeferredSubscription otherArbiter;

		final Subscriber<Object> signaller;

		final Publisher<? extends T> source;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<StreamRepeatWhenMainSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(StreamRepeatWhenMainSubscriber.class, "wip");

		volatile boolean cancelled;

		static final Object NEXT = new Object();

		public StreamRepeatWhenMainSubscriber(Subscriber<? super T> actual, Subscriber<Object> signaller,
												 Publisher<? extends T> source) {
			super(actual);
			this.signaller = signaller;
			this.source = source;
			this.otherArbiter = new DeferredSubscription();
		}

		@Override
		public void cancel() {
			if (cancelled) {
				return;
			}
			cancelled = true;

			cancelWhen();

			super.cancel();
		}

		void cancelWhen() {
			otherArbiter.cancel();
		}

		public void setWhen(Subscription w) {
			otherArbiter.set(w);
		}

		@Override
		public void onSubscribe(Subscription s) {
			set(s);
		}

		@Override
		public void onNext(T t) {
			subscriber.onNext(t);

			producedOne();
		}

		@Override
		public void onError(Throwable t) {
			otherArbiter.cancel();

			subscriber.onError(t);
		}

		@Override
		public void onComplete() {
			otherArbiter.request(1);

			signaller.onNext(NEXT);
		}

		void resubscribe() {
			if (WIP.getAndIncrement(this) == 0) {
				do {
					if (cancelled) {
						return;
					}

					source.subscribe(this);

				} while (WIP.decrementAndGet(this) != 0);
			}
		}

		void whenError(Throwable e) {
			cancelled = true;
			super.cancel();

			subscriber.onError(e);
		}

		void whenComplete() {
			cancelled = true;
			super.cancel();

			subscriber.onComplete();
		}
	}

	static final class StreamRepeatWhenOtherSubscriber 
	extends reactor.rx.Stream<Object>
	implements Subscriber<Object>, FeedbackLoop, Trace, Inner {
		StreamRepeatWhenMainSubscriber<?> main;

		final EmitterProcessor<Object> completionSignal = new EmitterProcessor<>();

		@Override
		public void onSubscribe(Subscription s) {
			main.setWhen(s);
			completionSignal.onSubscribe(EmptySubscription.INSTANCE);
		}

		@Override
		public void onNext(Object t) {
			main.resubscribe();
		}

		@Override
		public void onError(Throwable t) {
			main.whenError(t);
		}

		@Override
		public void onComplete() {
			main.whenComplete();
		}

		@Override
		public void subscribe(Subscriber<? super Object> s) {
			completionSignal.subscribe(s);
		}

		@Override
		public Object delegateInput() {
			return main;
		}

		@Override
		public Object delegateOutput() {
			return completionSignal;
		}
	}
}
