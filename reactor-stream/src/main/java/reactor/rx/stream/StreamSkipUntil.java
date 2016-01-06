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
import reactor.rx.subscriber.SerializedSubscriber;

/**
 * Skips values from the main publisher until the other publisher signals an onNext or onComplete.
 *
 * @param <T> the value type of the main Publisher
 * @param <U> the value type of the other Publisher
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 *
 * @since 2.5
 */
public final class StreamSkipUntil<T, U> extends StreamBarrier<T, T> {

	final Publisher<U> other;

	public StreamSkipUntil(Publisher<? extends T> source, Publisher<U> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		StreamSkipUntilMainSubscriber<T> mainSubscriber = new StreamSkipUntilMainSubscriber<>(s);

		StreamSkipUntilOtherSubscriber<U> otherSubscriber = new StreamSkipUntilOtherSubscriber<>(mainSubscriber);

		other.subscribe(otherSubscriber);

		source.subscribe(mainSubscriber);
	}

	static final class StreamSkipUntilOtherSubscriber<U> implements Subscriber<U> {

		final StreamSkipUntilMainSubscriber<?> main;

		public StreamSkipUntilOtherSubscriber(StreamSkipUntilMainSubscriber<?> main) {
			this.main = main;
		}

		@Override
		public void onSubscribe(Subscription s) {
			main.setOther(s);

			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(U t) {
			if (main.gate) {
				return;
			}
			StreamSkipUntilMainSubscriber<?> m = main;
			m.other.cancel();
			m.gate = true;
			m.other = CancelledSubscription.INSTANCE;
		}

		@Override
		public void onError(Throwable t) {
			StreamSkipUntilMainSubscriber<?> m = main;
			if (m.gate) {
				return;
			}
			m.onError(t);
		}

		@Override
		public void onComplete() {
			StreamSkipUntilMainSubscriber<?> m = main;
			if (m.gate) {
				return;
			}
			m.gate = true;
			m.other = CancelledSubscription.INSTANCE;
		}


	}

	static final class StreamSkipUntilMainSubscriber<T> implements Subscriber<T>, Subscription {

		final SerializedSubscriber<T> actual;

		volatile Subscription main;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<StreamSkipUntilMainSubscriber, Subscription> MAIN =
				AtomicReferenceFieldUpdater.newUpdater(StreamSkipUntilMainSubscriber.class, Subscription.class, "main");

		volatile Subscription other;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<StreamSkipUntilMainSubscriber, Subscription> OTHER =
				AtomicReferenceFieldUpdater.newUpdater(StreamSkipUntilMainSubscriber.class,
						Subscription.class,
						"other");

		volatile boolean gate;

		public StreamSkipUntilMainSubscriber(Subscriber<? super T> actual) {
			this.actual = new SerializedSubscriber<>(actual);
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
			if (gate) {
				actual.onNext(t);
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

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			cancelOther();

			actual.onComplete();
		}
	}
}
