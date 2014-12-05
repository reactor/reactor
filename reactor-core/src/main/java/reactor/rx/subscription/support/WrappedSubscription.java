/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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
package reactor.rx.subscription.support;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.rx.Stream;
import reactor.rx.subscription.PushSubscription;

/**
* @author Stephane Maldini
*/
public class WrappedSubscription<O> extends PushSubscription<O> {

	protected final Subscription subscription;
	protected final PushSubscription<O> pushSubscription;

	@SuppressWarnings("unchecked")
	public WrappedSubscription(Subscription subscription, Subscriber<? super O> subscriber) {
		super(null, subscriber);
		this.subscription = subscription;
		if (PushSubscription.class.isAssignableFrom(subscription.getClass())) {
			this.pushSubscription = (PushSubscription<O>) subscription;
		}else{
			this.pushSubscription = null;
		}
	}

	@Override
	public void request(long n) {
		if(pushSubscription != null){
			pushSubscription.request(n);
		}else{
			super.request(n);
		}
	}

	@Override
	public void cancel() {
		this.subscription.cancel();
		super.cancel();
	}

	@Override
	@SuppressWarnings("unchecked")
	public final Stream<O> getPublisher() {
		return PushSubscription.class.isAssignableFrom(this.subscription.getClass()) ?
				((PushSubscription<O>)this.subscription).getPublisher() :
				null;
	}

	@Override
	public void incrementCurrentNextSignals() {
		if(pushSubscription != null){
			pushSubscription.incrementCurrentNextSignals();
		}else{
			super.incrementCurrentNextSignals();
		}
	}

	@Override
	public void maxCapacity(long n) {
		if(pushSubscription != null){
			pushSubscription.maxCapacity(n);
		}
	}

	@Override
	public long clearPendingRequest() {
		if(pushSubscription != null){
			return pushSubscription.clearPendingRequest();
		}else{
			return super.clearPendingRequest();
		}
	}

	@Override
	public boolean shouldRequestPendingSignals() {
		if(pushSubscription != null){
			return pushSubscription.shouldRequestPendingSignals();
		}else{
			return super.shouldRequestPendingSignals();
		}
	}

	@Override
	public boolean isComplete() {
		if(pushSubscription != null){
			return pushSubscription.isComplete();
		}else{
			return super.isComplete();
		}
	}

	@Override
	protected final void onRequest(long elements) {
		this.subscription.request(elements);
	}

	@Override
	public final boolean equals(Object o) {
		return !(o == null || subscription.getClass() != o.getClass()) && subscription.equals(o);
	}

	@Override
	public final boolean hasPublisher() {
		return subscription != null;
	}

	@Override
	public final int hashCode() {
		return subscription.hashCode();
	}

	@Override
	public String toString() {
		return super.toString()+" wrapped="+subscription;
	}
}
