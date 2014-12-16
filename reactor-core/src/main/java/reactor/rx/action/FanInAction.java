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
package reactor.rx.action;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.function.Consumer;
import reactor.rx.Stream;
import reactor.rx.action.support.NonBlocking;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The best moment of my life so far, not.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
abstract public class FanInAction<I, E, O, SUBSCRIBER extends FanInAction.InnerSubscriber<I, E, O>> extends Action<E,
		O> {


	final FanInSubscription<I, E, O, SUBSCRIBER>     innerSubscriptions;
	final Iterable<? extends Publisher<? extends I>> composables;

	final static protected int NOT_STARTED = 0;
	final static protected int RUNNING     = 1;
	final static protected int COMPLETING  = 2;

	final AtomicInteger status = new AtomicInteger();


	DynamicMergeAction<?, ?> dynamicMergeAction = null;

	@SuppressWarnings("unchecked")
	public FanInAction(Dispatcher dispatcher) {
		this(dispatcher, null);
	}

	public FanInAction(Dispatcher dispatcher,
	                   Iterable<? extends Publisher<? extends I>> composables) {
		super(dispatcher);

		this.composables = composables;
		this.upstreamSubscription = this.innerSubscriptions = createFanInSubscription();
	}

	@Override
	public void subscribe(Subscriber<? super O> subscriber) {
		doSubscribe(this.innerSubscriptions);
		super.subscribe(subscriber);
	}

	public void addPublisher(Publisher<? extends I> publisher) {
		InnerSubscriber<I, E, O> inlineMerge = createSubscriber();
		inlineMerge.pendingRequests += innerSubscriptions.capacity();
		publisher.subscribe(inlineMerge);
	}

	public void scheduleCompletion() {
		if (status.compareAndSet(NOT_STARTED, COMPLETING)) {
			broadcastComplete();
		} else {
			if (innerSubscriptions.runningComposables == 0) {
				broadcastComplete();
			}
		}
	}

	@Override
	public void cancel() {
		if(dynamicMergeAction != null){
			dynamicMergeAction.cancel();
		}
		super.cancel();
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		if (status.compareAndSet(NOT_STARTED, RUNNING)) {
			innerSubscriptions.maxCapacity(capacity);
			if (composables != null) {
				if (innerSubscriptions.subscriptions.size() > 0) {
					innerSubscriptions.cancel();
					return;
				}
				capacity(initUpstreamPublisherAndCapacity());
			}
		}
	}

	protected long initUpstreamPublisherAndCapacity() {
		long maxCapacity = capacity;
		for (Publisher<? extends I> composable : composables) {
			if (Stream.class.isAssignableFrom(composable.getClass())) {
				maxCapacity = Math.min(maxCapacity, ((Stream<?>) composable).getCapacity());
			}
			addPublisher(composable);
		}
		return maxCapacity;
	}


	protected final boolean checkDynamicMerge() {
		return dynamicMergeAction != null && dynamicMergeAction.isPublishing();
	}

	@Override
	protected void doComplete() {
		status.set(COMPLETING);
		if (!checkDynamicMerge() && innerSubscriptions.runningComposables == 0) {
			cancel();
			broadcastComplete();
		}
	}

	@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		//	innerSubscriptions.request(elements);
		super.requestUpstream(capacity, terminated, elements);
		if ( dynamicMergeAction != null) {
			dynamicMergeAction.requestUpstream(capacity, terminated, elements );
		}
	}

	@Override
	public String toString() {
		return super.toString() +
				"{runningComposables=" + innerSubscriptions.runningComposables + "}";
	}

	protected FanInSubscription<I, E, O, SUBSCRIBER> createFanInSubscription() {
		return new FanInSubscription<I, E, O, SUBSCRIBER>(this);
	}

	@Override
	public FanInSubscription<I, E, O, SUBSCRIBER> getSubscription() {
		return innerSubscriptions;
	}

	protected abstract InnerSubscriber<I, E, O> createSubscriber();

	public abstract static class InnerSubscriber<I, E, O> implements Subscriber<I>, NonBlocking {
		final FanInAction<I, E, O, ? extends InnerSubscriber<I, E, O>> outerAction;
		FanInSubscription.InnerSubscription<I, E, InnerSubscriber<I, E, O>> s;

		long pendingRequests = 0;
		long emittedSignals  = 0;

		InnerSubscriber(FanInAction<I, E, O, ? extends InnerSubscriber<I, E, O>> outerAction) {
			this.outerAction = outerAction;
		}

		@SuppressWarnings("unchecked")
		void setSubscription(FanInSubscription.InnerSubscription s) {
			this.s = s;
		}

		public void start(){
			outerAction.innerSubscriptions.addSubscription(s);
			if(pendingRequests > 0){
				s.request(pendingRequests);
			}
			if(outerAction.dynamicMergeAction != null){
				outerAction.dynamicMergeAction.decrementWip();
			}
		}

		public void request(long n) {
			if (s == null || n <= 0) return;
			if((pendingRequests += n) < 0l){
				pendingRequests = Long.MAX_VALUE;
			}
			emittedSignals = 0;
			s.request(n);
		}

		@Override
		public void onError(Throwable t) {
			FanInSubscription.RUNNING_COMPOSABLE_UPDATER.decrementAndGet(outerAction.innerSubscriptions);
			outerAction.innerSubscriptions.onError(t);
		}

		@Override
		public void onComplete() {
			//Action.log.debug("event [complete] by: " + this);
			s.cancel();

			Consumer<Void> completeConsumer = new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					s.toRemove = true;
					long left = outerAction.innerSubscriptions.removeSubscription(s);
					if (left == 0 && !outerAction.checkDynamicMerge()) {
						outerAction.innerSubscriptions.onComplete();
					}

				}
			};

			outerAction.trySyncDispatch(null, completeConsumer);

		}

		@Override
		public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
			return outerAction.isReactivePull(dispatcher, producerCapacity);
		}

		@Override
		public long getCapacity() {
			return outerAction.capacity;
		}

		@Override
		public String toString() {
			return "FanInAction.InnerSubscriber{pending=" + pendingRequests + ", emitted=" + emittedSignals + "}";
		}
	}

}
