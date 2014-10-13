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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The best moment of my life so far, not.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
abstract public class FanInAction<I, E, O, SUBSCRIBER extends FanInAction.InnerSubscriber<I, E, O>> extends Action<E, O> {

	final FanInSubscription<I, E, SUBSCRIBER>           innerSubscriptions;
	final AtomicInteger                              runningComposables;
	final Iterable<? extends Publisher<? extends I>> composables;

	final AtomicBoolean started = new AtomicBoolean(false);

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
		this.innerSubscriptions = createFanInSubscription();
	}

	@Override
	public void subscribe(Subscriber<? super O> subscriber) {
		if (started.compareAndSet(false, true)) {
			onSubscribe(this.innerSubscriptions);
		}
		super.subscribe(subscriber);
	}

	public void addPublisher(Publisher<? extends I> publisher) {
		runningComposables.incrementAndGet();
		Subscriber<I> inlineMerge = createSubscriber();
		publisher.subscribe(inlineMerge);
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
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
				"{runningComposables=" + runningComposables + '}';
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
			if (n <= 0) return;

			int size = outerAction.runningComposables.get();
			if (size == 0) return;

			long batchSize = outerAction.capacity / size;
			long toRequest = outerAction.capacity % size + batchSize;
			toRequest = Math.min(toRequest, n);
			if (toRequest > 0) {
				pendingRequests += n - toRequest;
				emittedSignals = 0;
				s.request(toRequest);
			}
		}

		@Override
		public void onError(Throwable t) {
			outerAction.runningComposables.decrementAndGet();
			outerAction.innerSubscriptions.onError(t);
		}

		protected boolean checkDynamicMerge() {
			return outerAction.dynamicMergeAction != null && outerAction.dynamicMergeAction.upstreamSubscription != null;
		}

		@Override
		public String toString() {
			return "FanInAction.InnerSubscriber{pending=" + pendingRequests + ", emitted=" + emittedSignals + "}";
		}
	}

}
