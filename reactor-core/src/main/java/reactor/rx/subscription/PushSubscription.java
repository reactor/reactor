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
import reactor.function.Consumer;
import reactor.queue.CompletableQueue;
import reactor.rx.Stream;
import reactor.rx.action.support.SpecificationExceptions;
import reactor.rx.subscription.support.WrappedDropSubscription;
import reactor.rx.subscription.support.WrappedPushToReactiveSubscription;
import reactor.rx.subscription.support.WrappedSubscription;

/**
 * Relationship between a Stream (Publisher) and a Subscriber. A PushSubscription offers common facilities to track
 * downstream demand. Subclasses such as ReactiveSubscription implement these mechanisms to prevent Subscriber overrun.
 * <p>
 * In Reactor, a subscriber can be an Action which is both a Stream (Publisher) and a Subscriber.
 *
 * @author Stephane Maldini
 */
public class PushSubscription<O> implements Subscription,  Consumer<Long> {
	protected final Subscriber<? super O> subscriber;
	protected final           Stream<O>             publisher;

	protected volatile boolean terminated = false;

	protected long pendingRequestSignals = 0l;

	/**
	 * Wrap the subscription behind a push subscription to start tracking its requests
	 *
	 * @param subscription the subscription to wrap
	 * @return the new ReactiveSubscription
	 */
	public final static <O> PushSubscription<O> wrap(Subscription subscription, Subscriber<? super O> errorSubscriber) {
		return new WrappedSubscription<O>(subscription, errorSubscriber);
	}

	public PushSubscription(Stream<O> publisher, Subscriber<? super O> subscriber) {
		this.subscriber = subscriber;
		this.publisher = publisher;
	}

	/**
	 * Wrap the subscription behind a reactive subscription using the passed queue to buffer otherwise to drop rejected
	 * data.
	 *
	 * @param queue the optional queue to buffer overflow
	 * @return the new ReactiveSubscription
	 */
	public ReactiveSubscription<O> toReactiveSubscription(CompletableQueue<O> queue) {
		return new WrappedPushToReactiveSubscription<O>(this, queue);
	}

	/**
	 * Wrap the subscription behind a dropping subscription.
	 *
	 * @return the new DropSubscription
	 */
	public DropSubscription<O> toDropSubscription() {
		final PushSubscription<O> thiz = this;
		return new WrappedDropSubscription<>(thiz);
	}

	@Override
	public void accept(Long n) {
		request(n);
	}

	@Override
	public void request(long n) {
		try {
			if(publisher == null) {
				if (pendingRequestSignals != Long.MAX_VALUE && (pendingRequestSignals += n) < 0)
					subscriber.onError(SpecificationExceptions.spec_3_17_exception(pendingRequestSignals, n));
			}
			onRequest(n);
		} catch (Throwable t) {
			subscriber.onError(t);
		}

	}

	@Override
	public void cancel() {
		if(publisher != null){
			publisher.cleanSubscriptionReference(this);
		}

		terminated = true;
	}

	public void onComplete() {
		if (!terminated) {
			subscriber.onComplete();
			terminated = true;
		}
	}

	public void onNext(O ev) {
		if (!terminated) {
			subscriber.onNext(ev);
		}
	}

	public void onError(Throwable throwable) {
		subscriber.onError(throwable);
	}

	public Stream<O> getPublisher() {
		return publisher;
	}

	public boolean hasPublisher() {
		return publisher != null;
	}

	public void updatePendingRequests(long n) {
		if ((pendingRequestSignals += n) < 0) pendingRequestSignals = Long.MAX_VALUE;
	}


	public void doPendingRequest() {
		/*
		If we have to iterate over batch of requests, what to do on each flush happens here
		 */
	}

	protected void onRequest(long n){
		//IGNORE, full push
	}

	public final Subscriber<? super O> getSubscriber() {
		return subscriber;
	}

	public boolean isComplete() {
		return terminated;
	}

	public final long pendingRequestSignals() {
		return pendingRequestSignals;
	}

	public void incrementCurrentNextSignals(){
		/*
		Count operation for each data signal
		 */
	}

	public void maxCapacity(long n){
		/*
		Adjust capacity (usually the number of elements to be requested at most)
		 */
	}

	public boolean shouldRequestPendingSignals(){
		/*
		Should request the next batch of pending signals. Usually when current next signals reaches some limit like the maxCapacity.
		 */
		return false;
	}

	@Override
	public int hashCode() {
		int result = subscriber.hashCode();
		result = 31 * result + publisher.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		PushSubscription that = (PushSubscription) o;

		if (publisher.hashCode() != that.publisher.hashCode()) return false;
		if (!subscriber.equals(that.subscriber)) return false;

		return true;
	}

	@Override
	public String toString() {
		return "{push!}";
	}


}
