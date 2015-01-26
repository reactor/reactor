/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
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
package reactor.rx.subscription;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.fn.Consumer;
import reactor.rx.Stream;
import reactor.rx.subscription.support.WrappedSubscription;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Relationship between a Stream (Publisher) and a Subscriber. A PushSubscription offers common facilities to track
 * downstream demand. Subclasses such as ReactiveSubscription implement these mechanisms to prevent Subscriber overrun.
 * <p>
 * In Reactor, a subscriber can be an Action which is both a Stream (Publisher) and a Subscriber.
 *
 * @author Stephane Maldini
 */
public class PushSubscription<O> implements Subscription, Consumer<Long> {
	protected final Subscriber<? super O> subscriber;
	protected final Stream<O>             publisher;

	protected volatile int terminated = 0;

	protected static final AtomicIntegerFieldUpdater<PushSubscription> TERMINAL_UPDATER = AtomicIntegerFieldUpdater
			.newUpdater(PushSubscription.class, "terminated");


	protected volatile long pendingRequestSignals = 0l;

	protected static final AtomicLongFieldUpdater<PushSubscription> PENDING_UPDATER = AtomicLongFieldUpdater
			.newUpdater(PushSubscription.class, "pendingRequestSignals");


	/**
	 * Wrap the subscription behind a push subscription to start tracking its requests
	 *
	 * @param subscription the subscription to wrap
	 * @return the new ReactiveSubscription
	 */
	public static <O> PushSubscription<O> wrap(Subscription subscription, Subscriber<? super O> errorSubscriber) {
		return new WrappedSubscription<O>(subscription, errorSubscriber);
	}

	public PushSubscription(Stream<O> publisher, Subscriber<? super O> subscriber) {
		this.subscriber = subscriber;
		this.publisher = publisher;
	}

	@Override
	public final void accept(Long n) {
		request(n);
	}

	@Override
	public void request(long n) {
		try {
			if(publisher == null) {
				if (pendingRequestSignals != Long.MAX_VALUE && PENDING_UPDATER.addAndGet(this, n) < 0)
					PENDING_UPDATER.set(this, Long.MAX_VALUE);
			}
			onRequest(n);
		} catch (Throwable t) {
			subscriber.onError(t);
		}

	}

	@Override
	public void cancel() {
		TERMINAL_UPDATER.set(this, 1);
		if (publisher != null) {
			publisher.cleanSubscriptionReference(this);
		}
	}

	public boolean terminate(){
		return TERMINAL_UPDATER.compareAndSet(this, 0, 1);
	}

	public void onComplete() {
		if (TERMINAL_UPDATER.compareAndSet(this, 0, 1) && subscriber != null) {
			subscriber.onComplete();
		}
	}

	public void onNext(O ev) {
	//	if (terminated == 0) {
			subscriber.onNext(ev);
	//	}
	}

	public void onError(Throwable throwable) {
		if (TERMINAL_UPDATER.compareAndSet(this, 0, 1) && subscriber != null) {
			subscriber.onError(throwable);
		}
	}

	public Stream<O> getPublisher() {
		return publisher;
	}

	public boolean hasPublisher() {
		return publisher != null;
	}

	public void updatePendingRequests(long n) {
		long oldPending;
		long newPending;
		do{
			oldPending = pendingRequestSignals;
			newPending = n == 0l ? 0l : oldPending + n;
			if(newPending < 0) {
				newPending = n > 0 ? Long.MAX_VALUE : 0;
			}
		}while(!PENDING_UPDATER.compareAndSet(this, oldPending, newPending));
	}

	protected void onRequest(long n) {
		//IGNORE, full push
	}

	public final Subscriber<? super O> getSubscriber() {
		return subscriber;
	}

	public boolean isComplete() {
		return terminated == 1;
	}

	public final long pendingRequestSignals() {
		return pendingRequestSignals;
	}

	public void maxCapacity(long n) {
		/*
		Adjust capacity (usually the number of elements to be requested at most)
		 */
	}

	public boolean shouldRequestPendingSignals() {
		/*
		Should request the next batch of pending signals. Usually when current next signals reaches some limit like the
		maxCapacity.
		 */
		return false;
	}

	@Override
	public int hashCode() {
		int result = subscriber.hashCode();
		if(publisher != null){
			result = 31 * result + publisher.hashCode();
		}
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		PushSubscription that = (PushSubscription) o;

		if (publisher != null && publisher.hashCode() != that.publisher.hashCode()) return false;
		if (!subscriber.equals(that.subscriber)) return false;

		return true;
	}

	@Override
	public String toString() {
		return "{push"+
				(pendingRequestSignals > 0 && pendingRequestSignals != Long.MAX_VALUE ? ",pending="+pendingRequestSignals : "")
				+"}";
	}


}
