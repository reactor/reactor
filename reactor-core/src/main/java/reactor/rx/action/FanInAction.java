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
import reactor.rx.action.support.NonBlocking;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
abstract public class FanInAction<I, O> extends Action<I, O> {

	final FanInSubscription<I> innerSubscriptions;
	final AtomicInteger        runningComposables;

	Action<?, ?> masterAction = null;

	@SuppressWarnings("unchecked")
	public FanInAction(Dispatcher dispatcher) {
		this(dispatcher, null);
	}

	public FanInAction(Dispatcher dispatcher,
	                   List<? extends Publisher<I>> composables) {
		super(dispatcher);

		int length = composables != null ? composables.size() : 0;
		this.runningComposables = new AtomicInteger(0);

		if (length > 0) {
			this.innerSubscriptions = createFanInSubscription();

			onSubscribe(this.innerSubscriptions);

			for (Publisher<I> composable : composables) {
				addPublisher(composable);
			}
		} else {
			this.innerSubscriptions = createFanInSubscription();

			onSubscribe(this.innerSubscriptions);
		}

	}

	public void addPublisher(Publisher<I> publisher) {
		runningComposables.incrementAndGet();
		Subscriber<I> inlineMerge = createSubscriber();
		publisher.subscribe(inlineMerge);
	}

	@Override
	protected void requestUpstream(AtomicLong capacity, boolean terminated, long elements) {
		super.requestUpstream(capacity, terminated, elements);
		if (masterAction != null && masterAction.getState() == State.READY) {
			masterAction.requestUpstream(capacity, terminated, elements);
		}
	}

	@Override
	public FanInSubscription<I> getSubscription() {
		return innerSubscriptions;
	}

	@Override
	public String toString() {
		return super.toString() +
				"{runningComposables=" + runningComposables +
				'}';
	}

	protected FanInSubscription<I> createFanInSubscription() {
		return new FanInSubscription<I>(this,
				new ArrayList<FanInSubscription.InnerSubscription>(8)) {
			@Override
			public void cancel() {
				super.cancel();
				if (masterAction != null) {
					Action<?, ?> master = masterAction;
					masterAction = null;
					master.cancel();
				}
			}
		};
	}

	protected InnerSubscriber<I, O> createSubscriber() {
		return new InnerSubscriber<I, O>(this);
	}

	protected static class InnerSubscriber<I, O> implements Subscriber<I>, NonBlocking {
		final FanInAction<I, O> outerAction;
		FanInSubscription.InnerSubscription s;

		InnerSubscriber(FanInAction<I, O> outerAction) {
			this.outerAction = outerAction;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onSubscribe(final Subscription subscription) {
			this.s = new FanInSubscription.InnerSubscription(subscription);

			outerAction.innerSubscriptions.addSubscription(s);
			outerAction.dispatch(new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					long currentCapacity = outerAction.innerSubscriptions.getCapacity().get();
					if (currentCapacity > 0) {
						int size = outerAction.innerSubscriptions.subscriptions.size();
						if (size == 0) return;

						long batchSize = outerAction.capacity / size;
						long toRequest = outerAction.capacity % size + batchSize;
						toRequest = toRequest > currentCapacity ? toRequest : currentCapacity;
						s.request(toRequest);
					}
				}
			});


		}

		@Override
		public void onComplete() {
			if (outerAction.dispatcher.inContext()) {
				s.toRemove = true;
			} else {
				outerAction.innerSubscriptions.removeSubscription(s);
			}
			if (outerAction.runningComposables.decrementAndGet() == 0) {
				outerAction.innerSubscriptions.onComplete();
			}
		}

		@Override
		public void onError(Throwable t) {
			outerAction.runningComposables.decrementAndGet();
			outerAction.innerSubscriptions.onError(t);
		}

		@Override
		public void onNext(I ev) {
			outerAction.innerSubscriptions.onNext(ev);
		}

		@Override
		public String toString() {
			return "InnerSubscriber";
		}
	}

}
