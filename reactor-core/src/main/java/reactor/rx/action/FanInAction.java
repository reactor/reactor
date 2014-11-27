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
import reactor.event.dispatch.Dispatcher;
import reactor.rx.Stream;
import reactor.rx.action.support.NonBlocking;
import reactor.rx.subscription.PushSubscription;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

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

	volatile int runningComposables = 0;

	protected static final AtomicIntegerFieldUpdater<FanInAction> RUNNING_COMPOSABLE_UPDATER = AtomicIntegerFieldUpdater
			.newUpdater(FanInAction.class, "runningComposables");

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
		if (status.compareAndSet(NOT_STARTED, RUNNING)) {
			doSubscribe(this.innerSubscriptions);
		}
		super.subscribe(subscriber);
	}

	@Override
	protected PushSubscription<O> createSubscription(Subscriber<? super O> subscriber, boolean reactivePull) {
		//Already done with inner subscriptions
		return super.createSubscription(subscriber, false);
	}

	public void addPublisher(Publisher<? extends I> publisher) {
		RUNNING_COMPOSABLE_UPDATER.incrementAndGet(this);
		Subscriber<I> inlineMerge = createSubscriber();
		publisher.subscribe(inlineMerge);
	}

	@Override
	public void replayChildRequests(long pending) {
		if (pending == 0 && dynamicMergeAction != null && dynamicMergeAction.upstreamSubscription != null) {
			dynamicMergeAction.upstreamSubscription.clearPendingRequest();
			dynamicMergeAction.requestUpstream(capacity, innerSubscriptions.terminated, capacity);
		}
		else if (pending > 0) {
			requestMore(pending);
		}
	}

	public void scheduleCompletion() {
		if (status.compareAndSet(NOT_STARTED, COMPLETING)) {
			cancel();
			broadcastComplete();
		} else {
			status.set(COMPLETING);
			if (RUNNING_COMPOSABLE_UPDATER.get(this) == 0) {
				cancel();
				broadcastComplete();
			}
		}
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		innerSubscriptions.maxCapacity(capacity);
		if (composables != null) {
			if (innerSubscriptions.subscriptions.size() > 0) {
				innerSubscriptions.cancel();
				return;
			}
			capacity(initUpstreamPublisherAndCapacity());
		}
		super.doSubscribe(subscription);
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


	protected boolean checkDynamicMerge() {
		return dynamicMergeAction != null && !dynamicMergeAction.hasNoMorePublishers();
	}

	@Override
	protected void doComplete() {
		status.set(COMPLETING);
		if (RUNNING_COMPOSABLE_UPDATER.get(this) == 0) {
			cancel();
			broadcastComplete();
		}
	}

	@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		super.requestUpstream(capacity, terminated, elements);
		if (dynamicMergeAction != null && dynamicMergeAction.upstreamSubscription != null) {
				dynamicMergeAction.requestUpstream(capacity, terminated, elements);
		}
	}

	@Override
	public String toString() {
		return super.toString() +
				"{runningComposables=" + runningComposables + "}";
	}

	protected FanInSubscription<I, E, O, SUBSCRIBER> createFanInSubscription() {
		return new FanInSubscription<I, E, O, SUBSCRIBER>(this,
				new ArrayList<FanInSubscription.InnerSubscription<I, E, SUBSCRIBER>>(8)) {
			@Override
			public void cancel() {
				super.cancel();
				Action<?, ?> master = dynamicMergeAction;
				if (master != null) {
					dynamicMergeAction = null;
					master.cancel();
				}
			}
		};
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

		public void request(long n) {
			if (s == null || n <= 0) return;
			pendingRequests += n;
			emittedSignals = 0;
			s.request(n);
		}

		@Override
		public void onError(Throwable t) {
			RUNNING_COMPOSABLE_UPDATER.decrementAndGet(outerAction);
			outerAction.innerSubscriptions.onError(t);
		}

		@Override
		public Dispatcher getDispatcher() {
			return outerAction.dispatcher;
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
