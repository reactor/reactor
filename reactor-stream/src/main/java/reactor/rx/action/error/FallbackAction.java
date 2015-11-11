/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.rx.action.error;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.error.CancelException;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.Bounded;
import reactor.rx.action.Action;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class FallbackAction<T> extends Action<T, T> {

	protected final Publisher<? extends T> fallback;

	private volatile boolean switched        = false;
	private volatile long    pendingRequests = 0l;

	private FallbackSubscriber fallbackSubscriber;

	private static final AtomicLongFieldUpdater<FallbackAction> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(FallbackAction.class, "pendingRequests");

	public FallbackAction(Publisher<? extends T> fallback) {
		this.fallback = fallback;
	}

	@Override
	protected void doOnSubscribe(Subscription subscription) {
		long p = pendingRequests;
		if(p > 0){
			subscription.request(p);
		}
	}

	@Override
	protected void doNext(T ev) {

		if(switched){
			throw CancelException.get();
		}
		else{
			BackpressureUtils.getAndSub(REQUESTED, this, 1L);
			doNormalNext(ev);
		}
	}

	@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		FallbackSubscriber fallbackSubscriber = this.fallbackSubscriber;
		if(fallbackSubscriber == null) {
			BackpressureUtils.getAndAdd(REQUESTED, this, elements);
			super.requestUpstream(capacity, terminated, elements);
		}
		else{
			Subscription subscription = fallbackSubscriber.subscription;
			if(subscription != null){
				subscription.request(elements);
			}
		}
	}

	@Override
	public void cancel() {
		super.cancel();
		FallbackSubscriber fallbackSubscriber = this.fallbackSubscriber;
		if(fallbackSubscriber != null) {
			Subscription subscription = fallbackSubscriber.subscription;
			if(subscription != null){
				fallbackSubscriber.subscription = null;
				subscription.cancel();
			}
		}
	}

	protected void doSwitch(){
		if(!switched) {
			switched = true;
			cancel();
			fallbackSubscriber = new FallbackSubscriber();
			fallback.subscribe(fallbackSubscriber);
		}
	}

	protected void doNormalNext(T ev) {
		broadcastNext(ev);
	}

	protected void doFallbackNext(T ev) {
		broadcastNext(ev);
	}

	private class FallbackSubscriber extends BaseSubscriber<T> implements Bounded {

		Subscription subscription;

		@Override
		public void onSubscribe(Subscription s) {
			super.onSubscribe(s);
			subscription = s;
			long r = pendingRequests;
			if(r > 0){
				subscription.request(r);
			}
		}

		@Override
		public void onNext(T t) {
			super.onNext(t);
			BackpressureUtils.getAndSub(REQUESTED, FallbackAction.this, 1L);
			doFallbackNext(t);
		}

		@Override
		public void onError(Throwable t) {
			super.onError(t);
			broadcastError(t);
		}

		@Override
		public void onComplete() {
			super.onComplete();
			broadcastComplete();
		}

		@Override
		public boolean isExposedToOverflow(Bounded parentPublisher) {
			return FallbackAction.this.isExposedToOverflow(parentPublisher);
		}

		@Override
		public long getCapacity() {
			return FallbackAction.this.getCapacity();
		}
	}
}
