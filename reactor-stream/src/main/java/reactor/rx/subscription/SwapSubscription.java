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

import org.reactivestreams.Subscription;
import reactor.core.subscription.CancelledSubscription;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.core.support.internal.PlatformDependent;

/**
 * @author Stephane Maldini
 * @since 2.5
 */
public final class SwapSubscription<T> implements Subscription, ReactiveState.Upstream, ReactiveState.Trace {

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

	SwapSubscription() {
		SUBSCRIPTION.lazySet(this, EmptySubscription.INSTANCE);
	}

	/**
	 *
	 * @param subscription
	 */
	public void swapTo(Subscription subscription) {
		Subscription old = SUBSCRIPTION.getAndSet(this, subscription);
		if(old != EmptySubscription.INSTANCE){
			subscription.cancel();
			return;
		}
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
		return subscription == EmptySubscription.INSTANCE;
	}

	/**
	 *
	 * @param l
	 * @return
	 */
	public boolean ack(long l) {
		return BackpressureUtils.getAndSub(REQUESTED, this, l) >= l;
	}

	/**
	 *
	 * @return
	 */
	public boolean ack(){
		return BackpressureUtils.getAndSub(REQUESTED, this, 1L) != 0;
	}

	/**
	 *
	 * @return
	 */
	public boolean isCancelled(){
		return subscription == CancelledSubscription.INSTANCE;
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
			if(s == CancelledSubscription.INSTANCE || s == EmptySubscription.INSTANCE){
				return;
			}

			if(SUBSCRIPTION.compareAndSet(this, s, CancelledSubscription.INSTANCE)){
				s.cancel();
				break;
			}
		}
	}

	@Override
	public Object upstream() {
		return subscription;
	}

	@Override
	public String toString() {
		return "SwapSubscription{" +
				"subscription=" + subscription +
				", requested=" + requested +
				'}';
	}
}
