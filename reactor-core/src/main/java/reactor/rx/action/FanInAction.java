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
package reactor.rx.action;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;
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
abstract public class FanInAction<I, O, SUBSCRIBER extends FanInAction.InnerSubscriber<I, O>> extends Action<I, O> {

	final FanInSubscription<I, SUBSCRIBER>           innerSubscriptions;
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
			doSubscribe(this.innerSubscriptions);
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
		this.subscription = subscription;
		if (composables != null) {
			if (innerSubscriptions.subscriptions.size() > 0) {
				innerSubscriptions.cancel();
			}
			capacity(initUpstreamPublisherAndCapacity());
		}
		super.doSubscribe(subscription);
	}

	@Override
	public void subscribeWithSubscription(Subscriber<? super O> subscriber, StreamSubscription<O> subscription) {
		super.subscribeWithSubscription(subscriber, subscription);
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
		if (dynamicMergeAction != null && dynamicMergeAction.getState() == State.READY) {
			dynamicMergeAction.requestUpstream(capacity, terminated, elements);
		}
	}

	@Override
	public FanInSubscription<I, SUBSCRIBER> getSubscription() {
		return innerSubscriptions;
	}

	@Override
	public String toString() {
		return super.toString() +
				"{runningComposables=" + runningComposables +
				'}';
	}

	protected FanInSubscription<I, SUBSCRIBER> createFanInSubscription() {
		return new FanInSubscription<I, SUBSCRIBER>(this,
				new ArrayList<FanInSubscription.InnerSubscription<I, ? extends SUBSCRIBER>>(8)) {
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

	protected InnerSubscriber<I, O> createSubscriber() {
		return new InnerSubscriber<I, O>(this);
	}

	public static class InnerSubscriber<I, O> implements Subscriber<I>, NonBlocking {
		final FanInAction<I, O, ? extends InnerSubscriber<I, O>> outerAction;
		FanInSubscription.InnerSubscription<I, InnerSubscriber<I, O>> s;

		long pendingRequests = 0;
		long emittedSignals = 0;

		InnerSubscriber(FanInAction<I, O, ? extends InnerSubscriber<I, O>> outerAction) {
			this.outerAction = outerAction;
		}

		@SuppressWarnings("unchecked")
		void setSubscription(FanInSubscription.InnerSubscription s) {
			this.s = s;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onSubscribe(final Subscription subscription) {
			this.s = new FanInSubscription.InnerSubscription<I, InnerSubscriber<I, O>>(subscription, this);

			outerAction.innerSubscriptions.addSubscription(s);
			request(outerAction.innerSubscriptions.getCapacity().get());
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
		public void onNext(I ev) {
			//Action.log.debug("event [" + ev + "] by: " + this);
			emittedSignals++;
			outerAction.innerSubscriptions.onNext(ev);
			int size = outerAction.runningComposables.get();
			long batchSize = outerAction.capacity / size;
			if(emittedSignals >= batchSize){
				request(emittedSignals++);
			}
		}

		@Override
		public void onComplete() {
			//Action.log.debug("event [complete] by: " + this);
			Consumer<Void> completeConsumer = new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					if (!s.toRemove) {
						outerAction.innerSubscriptions.removeSubscription(s);
					}
					if (outerAction.runningComposables.decrementAndGet() == 0 && checkDynamicMerge()) {
						outerAction.innerSubscriptions.onComplete();
					}
				}
			};

			if (outerAction.dispatcher.inContext()) {
				s.toRemove = true;
				completeConsumer.accept(null);
			} else {
				outerAction.dispatch(completeConsumer);
			}

		}

		@Override
		public void onError(Throwable t) {
			outerAction.runningComposables.decrementAndGet();
			outerAction.innerSubscriptions.onError(t);
		}

		protected boolean checkDynamicMerge() {
			return outerAction.composables != null ||
					outerAction.dynamicMergeAction != null && !outerAction.dynamicMergeAction.checkState();
		}

		@Override
		public String toString() {
			return "FanInAction.InnerSubscriber{pending=" + pendingRequests + ", emitted="+emittedSignals+"}";
		}
	}

}
