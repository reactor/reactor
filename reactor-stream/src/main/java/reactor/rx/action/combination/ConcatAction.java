/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.rx.action.combination;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.core.reactivestreams.SerializedSubscriber;
import reactor.core.support.NonBlocking;
import reactor.rx.action.Action;
import reactor.rx.action.Signal;
import reactor.rx.subscription.PushSubscription;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
final public class ConcatAction<T> extends Action<Publisher<? extends T>, T> {

	private final ConcurrentLinkedQueue<Signal<Publisher<? extends T>>> queue;

	volatile int wip;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<ConcatAction> WIP_UPDATER = AtomicIntegerFieldUpdater.newUpdater
			(ConcatAction.class, "wip");

	// accessed by REQUESTED_UPDATER
	private volatile long requested;
	@SuppressWarnings("rawtypes")
	private static final AtomicLongFieldUpdater<ConcatAction> REQUESTED_UPDATER = AtomicLongFieldUpdater.newUpdater
			(ConcatAction.class, "requested");


	volatile ConcatInnerSubscriber currentSubscriber;

	public ConcatAction() {
		this.queue = new ConcurrentLinkedQueue<>();
	}

	@Override
	protected void doNext(Publisher<? extends T> ev) {
		queue.add(Signal.<Publisher<? extends T>>next(ev));

		if(WIP_UPDATER.getAndIncrement(this) == 0){
			subscribeNext();
		}
	}

	@Override
	public void onComplete() {
		try {
			queue.add(Signal.<Publisher<? extends T>>complete());
			if (WIP_UPDATER.getAndIncrement(this) == 0) {
				subscribeNext();
			}
		} catch (Exception e) {
			doError(e);
		}
	}

	@Override
	protected void doOnSubscribe(Subscription subscription) {
		requestMore(1L);
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		super.subscribe(SerializedSubscriber.create(subscriber));
	}

	@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		requestFromChild(elements);
	}

	private void requestFromChild(long n) {
		// we track 'requested' so we know whether we should subscribe the next or not
		if (REQUESTED_UPDATER.getAndAdd(this, n) == 0) {
			if (currentSubscriber == null && wip > 0) {
				// this means we may be moving from one subscriber to another after having stopped processing
				// so need to kick off the subscribe via this request notification
				subscribeNext();
				return;
			}
		}

		ConcatInnerSubscriber subscriber = currentSubscriber;
		if (subscriber != null) {
			// otherwise we are just passing it through to the currentSubscriber
			subscriber.requestMore(n);
		}
	}

	@Override
	protected void subscribeWithSubscription(Subscriber<? super T> subscriber, PushSubscription<T> subscription) {
		try {
			if (!addSubscription(subscription)) {
				subscriber.onError(new IllegalStateException("The subscription cannot be linked to this Stream"));
			} else {
				subscription.markAsDeferredStart();
				subscription.start();
			}
		} catch (Exception e) {
			subscriber.onError(e);
		}
	}

	@Override
	public void cancel() {
		queue.clear();
		super.cancel();
	}

	private void decrementRequested() {
		if (requested != Long.MAX_VALUE) {
			REQUESTED_UPDATER.decrementAndGet(this);
		}
	}

	void completeInner() {
		currentSubscriber = null;
		if (WIP_UPDATER.decrementAndGet(this) > 0) {
			subscribeNext();
		}
		requestMore(1L);
	}

	void subscribeNext() {
		if (requested > 0) {
			Signal<Publisher<? extends T>> o = queue.poll();
			if (o == null) return;
			if (o.isOnComplete()) {
				broadcastComplete();
			} else {
				Publisher<? extends T> source = o.get();
				currentSubscriber = new ConcatInnerSubscriber();
				source.subscribe(currentSubscriber);
			}
		} else {
			// requested == 0, so we'll peek to see if we are completed, otherwise wait until another request
			Signal<?> o = queue.peek();
			if (o != null && o.isOnComplete()) {
				broadcastComplete();
			}
		}
	}

	class ConcatInnerSubscriber implements Subscriber<T>, NonBlocking {

		private Subscription s;

		void requestMore(long n) {
			if (s != null) {
				s.request(n);
			}
		}

		@Override
		public void onSubscribe(Subscription s) {
			this.s = s;
			if (requested > 0) {
				s.request(requested);
			}
		}

		@Override
		public void onNext(T t) {
			ConcatAction.this.decrementRequested();
			broadcastNext(t);
		}

		@Override
		public void onError(Throwable e) {
			Subscription s = this.s;
			if(s != null){
				s.cancel();
			}
			ConcatAction.this.onError(e);
		}

		@Override
		public void onComplete() {
			Subscription s = this.s;
			if(s != null) {
				s.cancel();
			}
			ConcatAction.this.completeInner();
		}

		@Override
		public long getCapacity() {
			return Long.MAX_VALUE;
		}

		@Override
		public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
			return false;
		}
	}

}
