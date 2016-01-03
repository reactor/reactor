/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
package reactor.rx.stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.CancelException;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.support.BackpressureUtils;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.5
 */
public abstract class StreamFallback<T> extends StreamBarrier<T, T> {

	protected final Publisher<? extends T> fallback;

	public StreamFallback(Publisher<T> source, Publisher<? extends T> fallback) {
		super(source);
		this.fallback = fallback;
	}

	protected static class FallbackAction<T> extends SubscriberWithDemand<T, T> {

		protected final Publisher<? extends T> fallback;

		private volatile boolean switched        = false;
		private FallbackSubscriber fallbackSubscriber;

		public FallbackAction(Subscriber<? super T> actual, Publisher<? extends T> fallback) {
			super(actual);
			this.fallback = fallback;
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
		protected void doRequest(long elements) {
			FallbackSubscriber fallbackSubscriber = this.fallbackSubscriber;
			if(fallbackSubscriber == null) {
				doRequested(BackpressureUtils.getAndAdd(REQUESTED, this, elements), elements);
			}
			else{
				Subscription subscription = fallbackSubscriber.subscription;
				if(subscription != null){
					subscription.request(elements);
				}
			}
		}

		@Override
		protected void checkedCancel() {
			super.checkedCancel();
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
			subscriber.onNext(ev);
		}

		protected void doFallbackNext(T ev) {
			subscriber.onNext(ev);
		}

		private class FallbackSubscriber extends BaseSubscriber<T> implements Inner, FeedbackLoop{

			Subscription subscription;

			@Override
			public void onSubscribe(Subscription s) {
				super.onSubscribe(s);
				subscription = s;
				long r = requestedFromDownstream();
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
				checkedError(t);
			}

			@Override
			public void onComplete() {
				super.onComplete();
				checkedComplete();
			}

			@Override
			public Object delegateInput() {
				return FallbackAction.this;
			}

			@Override
			public Object delegateOutput() {
				return null;
			}
		}
	}

}
