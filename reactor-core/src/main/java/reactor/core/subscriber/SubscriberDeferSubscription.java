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

package reactor.core.subscriber;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.subscription.CancelledSubscription;
import reactor.core.support.BackpressureUtils;

/**
 * Arbitrates the requests and cancellation for a Subscription that may be set onSubscribe once only.
 * <p>
 * Note that {@link #request(long)} doesn't validate the amount.
 * <p>
 * {@see https//github.com/reactor/reactive-streams-commons}
 *
 * @since 2.5
 */
public class SubscriberDeferSubscription<I, O> implements Subscription, Subscriber<I> {

	protected final Subscriber<? super O> subscriber;

	volatile Subscription s;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<SubscriberDeferSubscription, Subscription> S =
			AtomicReferenceFieldUpdater.newUpdater(SubscriberDeferSubscription.class, Subscription.class, "s");

	volatile long requested;
	@SuppressWarnings("rawtypes")
	static final AtomicLongFieldUpdater<SubscriberDeferSubscription> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(SubscriberDeferSubscription.class, "requested");

	/**
	 * Constructs a SingleSubscriptionArbiter with zero initial request.
	 */
	public SubscriberDeferSubscription(Subscriber<? super O> subscriber) {
		this.subscriber = subscriber;
	}

	/**
	 * Constructs a SingleSubscriptionArbiter with the specified initial request amount.
	 *
	 * @param initialRequest
	 *
	 * @throws IllegalArgumentException if initialRequest is negative
	 */
	public SubscriberDeferSubscription(Subscriber<? super O> subscriber, long initialRequest) {
		if (initialRequest < 0) {
			throw new IllegalArgumentException("initialRequest >= required but it was " + initialRequest);
		}
		this.subscriber = subscriber;
		REQUESTED.lazySet(this, initialRequest);
	}

	/**
	 * Atomically sets the single subscription and requests the missed amount from it.
	 *
	 * @param s
	 *
	 * @return false if this arbiter is cancelled or there was a subscription already set
	 */
	public final boolean set(Subscription s) {
		Objects.requireNonNull(s, "s");
		Subscription a = this.s;
		if (a == CancelledSubscription.INSTANCE) {
			s.cancel();
			return false;
		}
		if (a != null) {
			s.cancel();
			return false;
		}

		if (S.compareAndSet(this, null, s)) {

			long r = REQUESTED.getAndSet(this, 0L);

			if (r != 0L) {
				s.request(r);
			}

			return true;
		}

		a = this.s;

		if (a != CancelledSubscription.INSTANCE) {
			s.cancel();
		}

		return false;
	}

	@Override
	public void request(long n) {
		Subscription a = s;
		if (a != null) {
			a.request(n);
		}
		else {
			BackpressureUtils.addAndGet(REQUESTED, this, n);

			a = s;

			if (a != null) {
				long r = REQUESTED.getAndSet(this, 0L);

				if (r != 0L) {
					a.request(r);
				}
			}
		}
	}

	@Override
	public void cancel() {
		Subscription a = s;
		if (a != CancelledSubscription.INSTANCE) {
			a = S.getAndSet(this, CancelledSubscription.INSTANCE);
			if (a != null && a != CancelledSubscription.INSTANCE) {
				a.cancel();
			}
		}
	}

	/**
	 * Returns true if this arbiter has been cancelled.
	 *
	 * @return true if this arbiter has been cancelled
	 */
	public final boolean isCancelled() {
		return s == CancelledSubscription.INSTANCE;
	}

	/**
	 * Returns true if a subscription has been set or the arbiter has been cancelled.
	 * <p>
	 * Use {@link #isCancelled()} to distinguish between the two states.
	 *
	 * @return true if a subscription has been set or the arbiter has been cancelled
	 */
	public final boolean hasSubscription() {
		return s != null;
	}

	@Override
	public void onSubscribe(Subscription s) {
		set(s);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onNext(I t) {
		if (subscriber != null) {
			subscriber.onNext((O) t);
		}
	}

	@Override
	public void onError(Throwable t) {
		if (subscriber != null) {
			subscriber.onError(t);
		}
	}

	@Override
	public void onComplete() {
		if (subscriber != null) {
			subscriber.onComplete();
		}
	}
}
