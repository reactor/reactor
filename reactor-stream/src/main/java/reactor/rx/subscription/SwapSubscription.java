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

package reactor.rx.subscription;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.Publishable;
import reactor.core.support.internal.PlatformDependent;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class SwapSubscription<T> implements Subscription, Publishable<T> {

	@SuppressWarnings("unused")
	private volatile Subscription subscription;
	private static final AtomicReferenceFieldUpdater<SwapSubscription, Subscription> SUBSCRIPTION =
			PlatformDependent.newAtomicReferenceFieldUpdater(SwapSubscription.class, "subscription");


	@SuppressWarnings("unused")
	private volatile long requested;
	protected static final AtomicLongFieldUpdater<SwapSubscription> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(SwapSubscription.class, "requested");

	public static <T> SwapSubscription<T> create(){
		return new SwapSubscription<>();
	}

	public SwapSubscription() {
		SUBSCRIPTION.lazySet(this, UNSUBSCRIBED);
	}

	static private final Subscription UNSUBSCRIBED = new Subscription() {
		@Override
		public void request(long n) {
			//IGNORE;
		}

		@Override
		public void cancel() {
			//IGNORE;
		}
	};

	static private final Subscription CANCELLED = new Subscription() {
		@Override
		public void request(long n) {
			//IGNORE;
		}

		@Override
		public void cancel() {
			//IGNORE;
		}
	};

	/**
	 *
	 * @param subscription
	 */
	public void swapTo(Subscription subscription) {
		Subscription old = SUBSCRIPTION.getAndSet(this, subscription);
		old.cancel();
		long r = REQUESTED.getAndSet(this, 0L);
		if(r != 0L){
			subscription.request(r);
		}
	}

	/**
	 *
	 * @return
	 */
	public boolean isUnsubscribed(){
		return subscription == UNSUBSCRIBED;
	}

	/**
	 *
	 * @return
	 */
	public boolean isCancelled(){
		return subscription == CANCELLED;
	}

	@Override
	public void request(long n) {
		BackpressureUtils.getAndAdd(REQUESTED, this, n);
		SUBSCRIPTION.get(this)
		            .request(n);
	}

	@Override
	public void cancel() {
		Subscription s;
		for(;;) {
			s = subscription;
			if(s == CANCELLED || s == UNSUBSCRIBED){
				return;
			}

			if(SUBSCRIPTION.compareAndSet(this, s, CANCELLED)){
				s.cancel();
				break;
			}
		}
	}

	@Override
	public Publisher<T> upstream() {
		return Publishers.fromSubscription(subscription);
	}
}
