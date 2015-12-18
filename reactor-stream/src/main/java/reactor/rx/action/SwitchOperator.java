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
package reactor.rx.action;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.subscriber.SerializedSubscriber;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class SwitchOperator<T> implements Publishers.Operator<Publisher<? extends T>, T> {

	public final static SwitchOperator INSTANCE = new SwitchOperator();

	@Override
	public Subscriber<? super Publisher<? extends T>> apply(Subscriber<? super T> subscriber) {
		return new SwitchAction<>(SerializedSubscriber.create(subscriber));
	}

	static final class SwitchAction<T> extends SubscriberWithDemand<Publisher<? extends T>, T> {

		private SwitchSubscriber switchSubscriber;

		public SwitchAction(Subscriber<? super T> subscriber) {
			super(subscriber);
		}

		public SwitchSubscriber getSwitchSubscriber() {
			return switchSubscriber;
		}

		@Override
		protected void doOnSubscribe(Subscription s) {
			subscriber.onSubscribe(this);

			final SwitchSubscriber switcher;
			final boolean toSubscribe;
			synchronized (this) {
				switcher = switchSubscriber;
				toSubscribe = switcher != null && switcher.s == null;
			}
			if (toSubscribe) {
				switcher.publisher.subscribe(switcher);
			}
		}

		@Override
		protected void checkedCancel() {
			SwitchSubscriber subscriber;
			synchronized (this) {
				subscriber = switchSubscriber;
			}
			if (subscriber != null) {
				subscriber.cancel();
			}
			super.checkedCancel();
		}

		@Override
		protected void doNext(Publisher<? extends T> ev) {
			SwitchSubscriber subscriber, nextSubscriber;
			synchronized (this) {
				if (switchSubscriber != null && switchSubscriber.publisher == ev) return;
				BackpressureUtils.getAndSub(REQUESTED, this, -1L);
				subscriber = switchSubscriber;
				switchSubscriber = nextSubscriber = new SwitchSubscriber(ev);
			}

			if (subscriber != null) {
				subscriber.cancel();
			}

			ev.subscribe(nextSubscriber);
		}

		@Override
		protected void doTerminate() {
			SwitchSubscriber subscriber;
			synchronized (this) {
				subscriber = switchSubscriber;
				if (subscriber != null) {
					switchSubscriber = null;
				}
			}
			if (subscriber != null) {
				subscriber.cancel();
			}
		}

		@Override
		protected void doRequested(long before,long elements) {
			SwitchSubscriber subscriber = switchSubscriber;
			requestMore(elements);
			if (subscriber != null) {
				subscriber.request(elements);
			}
		}

		public class SwitchSubscriber implements ReactiveState.Bounded, Subscriber<T>, Subscription {
			final Publisher<? extends T> publisher;

			Subscription s;

			public SwitchSubscriber(Publisher<? extends T> publisher) {
				this.publisher = publisher;
			}

			@Override
			public long getCapacity() {
				return SwitchAction.this.getCapacity();
			}

			@Override
			public void onSubscribe(final Subscription s) {
				this.s = s;
				long pending = requestedFromDownstream();
				if (pending > 0 && !isTerminated()) {
					s.request(pending);
				}
			}

			@Override
			public void onNext(T t) {
				try {
					subscriber.onNext(t);
				}
				catch (Throwable e){
					SwitchAction.this.onError(e);
				}

			}

			@Override
			public void onError(Throwable t) {
				s = null;
				doCancel();
				subscriber.onError(t);
			}

			@Override
			public void onComplete() {
				synchronized (SwitchAction.this) {
					switchSubscriber = null;
				}

				s = null;
				if(subscription == null){
					subscriber.onComplete();
				}
			}

			@Override
			public void request(long n) {
				s.request(n);
			}

			public void cancel() {
				Subscription s = this.s;
				if (s != null) {
					this.s = null;
					s.cancel();
				}
			}

			public Subscription getSubscription() {
				return s;
			}
		}
	}
}
