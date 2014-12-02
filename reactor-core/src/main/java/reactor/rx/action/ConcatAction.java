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

import java.util.ArrayList;
import java.util.List;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
final public class ConcatAction<O> extends FanInAction<O, O, O, ConcatAction.InnerSubscriber<O>> {

	public ConcatAction(Dispatcher dispatcher) {
		super(dispatcher);
	}

	public ConcatAction(Dispatcher dispatcher, List<? extends Publisher<? extends O>> composables) {
		super(dispatcher, composables);
	}

	@Override
	protected void doNext(O ev) {
		broadcastNext(ev);
	}

	protected InnerSubscriber<O> createSubscriber() {
		return new InnerSubscriber<O>(this);
	}


	@Override
	protected FanInSubscription<O, O, O, InnerSubscriber<O>> createFanInSubscription() {
		return new ConcatSubscription(this,
				new ArrayList<FanInSubscription.InnerSubscription<O, O, InnerSubscriber<O>>>(8));
	}

	/*@Override
	protected PushSubscription<O> createTrackingSubscription(Subscription subscription) {
		return innerSubscriptions;
	}
	*/

	public static final class InnerSubscriber<I> extends FanInAction.InnerSubscriber<I, I, I> {

		InnerSubscriber(FanInAction<I, I, I, ? extends FanInAction.InnerSubscriber<I, I, I>> outerAction) {
			super(outerAction);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onSubscribe(final Subscription subscription) {
			this.s = new FanInSubscription.InnerSubscription<I, I, FanInAction.InnerSubscriber<I, I, I>>(subscription, this);

			start();
		}

		@Override
		public void onNext(I ev) {
			//Action.log.debug("event [" + ev + "] by: " + this);
			outerAction.innerSubscriptions.onNext(ev);
			emittedSignals++;
			if(--pendingRequests < 0) pendingRequests = 0;
		}

		@Override
		public String toString() {
			return "Concat.InnerSubscriber{pending=" + pendingRequests + ", emitted=" + emittedSignals + "}";
		}
	}

	private final class ConcatSubscription extends FanInSubscription<O, O, O, InnerSubscriber<O>> {

		InnerSubscription<O, O, InnerSubscriber<O>> current = null;

		public ConcatSubscription(Subscriber<? super O> subscriber,
		                       List<InnerSubscription<O, O, InnerSubscriber<O>>> subs) {
			super(subscriber, subs);
			if(subs != null && subs.size() > 0){
				current = subs.get(0);
			}
		}

		@Override
		void removeSubscription(InnerSubscription s) {
			lock.writeLock().lock();
			try {
				subscriptions.remove(s);
				if(!subscriptions.isEmpty()){
					current = subscriptions.get(0);
					if(pendingRequestSignals > 0){
						current.request(pendingRequestSignals);
					}
				}
			} finally {
				lock.writeLock().unlock();
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		void addSubscription(InnerSubscription s) {
			lock.writeLock().lock();
			try {
				if(subscriptions.isEmpty()){
					current = s;
				}
				subscriptions.add(s);
			} finally {
				lock.writeLock().unlock();
			}
		}

		@Override
		protected void parallelRequest(long elements) {
			lock.writeLock().lock();
			try {
				if(current != null){
					current.request(elements);
				}else{
					updatePendingRequests(elements);
				}

				if(terminated){
					cancel();
				}
			} finally {
				lock.writeLock().unlock();
			}
		}
	}

}
