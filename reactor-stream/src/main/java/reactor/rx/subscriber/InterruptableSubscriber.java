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
package reactor.rx.subscriber;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.error.CancelException;
import reactor.core.subscriber.ConsumerSubscriber;
import reactor.core.support.ReactiveStateUtils;
import reactor.core.support.internal.PlatformDependent;
import reactor.fn.Consumer;
import reactor.rx.action.Control;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public class InterruptableSubscriber<T> extends ConsumerSubscriber<T> implements Control {


	@SuppressWarnings("unused")
	volatile Subscription subscription;
	final static AtomicReferenceFieldUpdater<InterruptableSubscriber, Subscription> SUBSCRIPTION =
			PlatformDependent.newAtomicReferenceFieldUpdater(InterruptableSubscriber.class, "subscription");

	static final Subscription CANCELLED = new Subscription() {
		@Override
		public void request(long n) {

		}

		@Override
		public void cancel() {

		}
	};
	static final Subscription UNSUBSCRIBED = new Subscription() {
		@Override
		public void request(long n) {

		}

		@Override
		public void cancel() {

		}
	};


	public InterruptableSubscriber(Consumer<? super T> consumer, Consumer<? super Throwable> errorConsumer,
			Consumer<Void> completeConsumer) {
		super(consumer, errorConsumer, completeConsumer);
		SUBSCRIPTION.lazySet(this, UNSUBSCRIBED);
	}

	@Override
	public void cancel() {
		if(SUBSCRIPTION.getAndSet(this, CANCELLED) != CANCELLED) {
			super.cancel();
		}
	}

	@Override
	protected final void doNext(T x) {
		if(subscription == CANCELLED){
			throw CancelException.get();
		}
		super.doNext(x);
		doPostNext(x);
	}

	@Override
	protected final void doSubscribe(Subscription s) {
		if(SUBSCRIPTION.getAndSet(this, s) != CANCELLED) {
			doSafeSubscribe(s);
		}
	}

	@Override
	protected final void doError(Throwable t) {
		if(SUBSCRIPTION.getAndSet(this, CANCELLED) != CANCELLED) {
			doSafeError(t);
		}
	}

	@Override
	protected final void doComplete() {
		if(SUBSCRIPTION.getAndSet(this, CANCELLED) != CANCELLED) {
			doSafeComplete();
		}
	}

	@Override
	protected void requestMore(long n) {
		Subscription subscription = SUBSCRIPTION.get(this);
		if(subscription != UNSUBSCRIBED){
			subscription.request(n);
		}
	}

	protected void doSafeSubscribe(Subscription s){
		super.doSubscribe(s);
	}

	protected void doPostNext(T x) {

	}

	protected void doSafeComplete() {
		super.doComplete();
	}

	protected void doSafeError(Throwable t) {
		super.doError(t);
	}

	@Override
	public boolean isTerminated() {
		return SUBSCRIPTION.get(this) == CANCELLED ;
	}

	@Override
	public ReactiveStateUtils.Graph debug() {
		return ReactiveStateUtils.scan(this);
	}

	@Override
	public boolean isStarted() {
		return SUBSCRIPTION.get(this) != UNSUBSCRIBED ;
	}
}
