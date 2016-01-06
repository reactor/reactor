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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.subscription.CancelledSubscription;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.BackpressureUtils;
import reactor.fn.BiFunction;
import reactor.rx.subscriber.SerializedSubscriber;

/**
 * Combines values from a main Publisher with values from another Publisher through a bi-function and emits the result.
 * <p>
 * <p>
 * The operator will drop values from the main source until the other Publisher produces any value.
 * <p>
 * If the other Publisher completes without any value, the sequence is completed.
 *
 * @param <T> the main source type
 * @param <U> the alternate source type
 * @param <R> the output type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 *
 * @since 2.5
 */
public final class StreamWithLatestFrom<T, U, R> extends StreamBarrier<T, R> {

	final Publisher<? extends U> other;

	final BiFunction<? super T, ? super U, ? extends R> combiner;

	public StreamWithLatestFrom(Publisher<? extends T> source,
			Publisher<? extends U> other,
			BiFunction<? super T, ? super U, ? extends R> combiner) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
		this.combiner = Objects.requireNonNull(combiner, "combiner");
	}

	@Override
	public void subscribe(Subscriber<? super R> s) {
		SerializedSubscriber<R> serial = new SerializedSubscriber<>(s);

		StreamWithLatestFromSubscriber<T, U, R> main = new StreamWithLatestFromSubscriber<>(serial, combiner);

		StreamWithLatestFromOtherSubscriber<U> secondary = new StreamWithLatestFromOtherSubscriber<>(main);

		other.subscribe(secondary);

		source.subscribe(main);
	}

	static final class StreamWithLatestFromSubscriber<T, U, R> implements Subscriber<T>, Subscription {

		final Subscriber<? super R> actual;

		final BiFunction<? super T, ? super U, ? extends R> combiner;

		volatile Subscription main;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<StreamWithLatestFromSubscriber, Subscription> MAIN =
				AtomicReferenceFieldUpdater.newUpdater(StreamWithLatestFromSubscriber.class,
						Subscription.class,
						"main");

		volatile Subscription other;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<StreamWithLatestFromSubscriber, Subscription> OTHER =
				AtomicReferenceFieldUpdater.newUpdater(StreamWithLatestFromSubscriber.class,
						Subscription.class,
						"other");

		volatile U otherValue;

		public StreamWithLatestFromSubscriber(Subscriber<? super R> actual,
				BiFunction<? super T, ? super U, ? extends R> combiner) {
			this.actual = actual;
			this.combiner = combiner;
		}

		void setOther(Subscription s) {
			if (!OTHER.compareAndSet(this, null, s)) {
				s.cancel();
				if (other != CancelledSubscription.INSTANCE) {
					BackpressureUtils.reportSubscriptionSet();
				}
			}
		}

		@Override
		public void request(long n) {
			main.request(n);
		}

		void cancelMain() {
			Subscription s = main;
			if (s != CancelledSubscription.INSTANCE) {
				s = MAIN.getAndSet(this, CancelledSubscription.INSTANCE);
				if (s != null && s != CancelledSubscription.INSTANCE) {
					s.cancel();
				}
			}
		}

		void cancelOther() {
			Subscription s = other;
			if (s != CancelledSubscription.INSTANCE) {
				s = OTHER.getAndSet(this, CancelledSubscription.INSTANCE);
				if (s != null && s != CancelledSubscription.INSTANCE) {
					s.cancel();
				}
			}
		}

		@Override
		public void cancel() {
			cancelMain();
			cancelOther();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (!MAIN.compareAndSet(this, null, s)) {
				s.cancel();
				if (main != CancelledSubscription.INSTANCE) {
					BackpressureUtils.reportSubscriptionSet();
				}
			}
			else {
				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			U u = otherValue;

			if (u != null) {
				R r;

				try {
					r = combiner.apply(t, u);
				}
				catch (Throwable e) {
					onError(e);
					return;
				}

				if (r == null) {
					onError(new NullPointerException("The combiner returned a null value"));
					return;
				}

				actual.onNext(r);
			}
			else {
				main.request(1);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (main == null) {
				if (MAIN.compareAndSet(this, null, CancelledSubscription.INSTANCE)) {
					EmptySubscription.error(actual, t);
					return;
				}
			}
			cancel();

			otherValue = null;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			cancelOther();

			otherValue = null;
			actual.onComplete();
		}

	}

	static final class StreamWithLatestFromOtherSubscriber<U> implements Subscriber<U> {

		final StreamWithLatestFromSubscriber<?, U, ?> main;

		public StreamWithLatestFromOtherSubscriber(StreamWithLatestFromSubscriber<?, U, ?> main) {
			this.main = main;
		}

		@Override
		public void onSubscribe(Subscription s) {
			main.setOther(s);

			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(U t) {
			main.otherValue = t;
		}

		@Override
		public void onError(Throwable t) {
			main.onError(t);
		}

		@Override
		public void onComplete() {
			StreamWithLatestFromSubscriber<?, U, ?> m = main;
			if (m.otherValue == null) {
				m.cancelMain();

				m.onComplete();
			}
		}


	}
}
