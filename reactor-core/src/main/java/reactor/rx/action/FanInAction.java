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

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The best moment of my life so far, not.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
abstract public class FanInAction<I, E, O, SUBSCRIBER extends FanInAction.InnerSubscriber<I, E, O>> extends Action<E, O> {




	final FanInSubscription<I, E, SUBSCRIBER>        innerSubscriptions;
	final AtomicInteger                              runningComposables;
	final Iterable<? extends Publisher<? extends I>> composables;

	final static protected int NOT_STARTED = 0;
	final static protected int RUNNING     = 1;
	final static protected int COMPLETING  = 2;

	final AtomicInteger status = new AtomicInteger();

	Action<?, ?> dynamicMergeAction = null;

	@SuppressWarnings("unchecked")
	public FanInAction(Dispatcher dispatcher) {
		this(dispatcher, null);
	}

	public FanInAction(Dispatcher dispatcher,
	                   Iterable<? extends Publisher<? extends I>> composables) {
		super(dispatcher);

		this.composables = composables;
		this.runningComposables = new AtomicInteger(0);
		this.upstreamSubscription = this.innerSubscriptions = createFanInSubscription();
	}

	@Override
	public void subscribe(Subscriber<? super O> subscriber) {
		if (status.compareAndSet(NOT_STARTED, RUNNING)) {
			doSubscribe(this.innerSubscriptions);
		}
		super.subscribe(subscriber);
	}

	public void addPublisher(Publisher<? extends I> publisher) {
		runningComposables.incrementAndGet();
		Subscriber<I> inlineMerge = createSubscriber();
		publisher.subscribe(inlineMerge);
	}

	public void scheduleCompletion(){
		if(status.compareAndSet(NOT_STARTED, COMPLETING)) {
			cancel();
			broadcastComplete();
		} else {
			status.set(COMPLETING);
			if (runningComposables.get() == 0) {
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
		return dynamicMergeAction != null && dynamicMergeAction.upstreamSubscription != null;
	}

	@Override
	protected void doComplete() {
		status.set(COMPLETING);
		if (runningComposables.get() == 0) {
			cancel();
			broadcastComplete();
		}
	}

	@Override
	protected void requestUpstream(AtomicLong capacity, boolean terminated, long elements) {
		super.requestUpstream(capacity, terminated, elements);
		if (dynamicMergeAction != null) {
			dynamicMergeAction.requestUpstream(capacity, terminated, elements);
		}
	}

	@Override
	public String toString() {
		return super.toString() +
				"{runningComposables=" + runningComposables + "}";
	}

	protected FanInSubscription<I, E, SUBSCRIBER> createFanInSubscription(){
		return new FanInSubscription<I, E, SUBSCRIBER>(this,
				new ArrayList<FanInSubscription.InnerSubscription<I, E, ? extends SUBSCRIBER>>(8)) {
			@Override
			public void cancel() {
				super.cancel();
				if (dynamicMergeAction != null) {
					Action<?, ?> master = dynamicMergeAction;
					dynamicMergeAction = null;
					master.cancel();
				}
			}
		};
	}

	@Override
	public  FanInSubscription<I, E, SUBSCRIBER>  getSubscription() {
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
			outerAction.runningComposables.decrementAndGet();
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
