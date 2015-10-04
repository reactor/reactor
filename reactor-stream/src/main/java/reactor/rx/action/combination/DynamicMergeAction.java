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
package reactor.rx.action.combination;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.support.BackpressureUtils;
import reactor.rx.action.Action;
import reactor.rx.subscription.PushSubscription;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class DynamicMergeAction<I, O> extends Action<Publisher<? extends I>, O> {

	private final FanInAction<I, ?, O, ? extends FanInAction.InnerSubscriber<I, ?, O>> fanInAction;
	private volatile int wip = 0;

	protected static final AtomicIntegerFieldUpdater<DynamicMergeAction> WIP_UPDATER = AtomicIntegerFieldUpdater
			.newUpdater(DynamicMergeAction.class, "wip");

	private volatile long requested = 0;

	protected static final AtomicLongFieldUpdater<DynamicMergeAction> REQUESTED_UPDATER = AtomicLongFieldUpdater
			.newUpdater(DynamicMergeAction.class, "requested");


	public DynamicMergeAction(){
		this(null);
	}

	@SuppressWarnings("unchecked")
	public DynamicMergeAction(
			FanInAction<I, ?, O, ? extends FanInAction.InnerSubscriber<I, ?, O>> fanInAction
	) {
		this.fanInAction = fanInAction == null ?
				(FanInAction<I, ?, O, ? extends FanInAction.InnerSubscriber<I, ?, O>>) new MergeAction<O>
						() :
				fanInAction;

		this.fanInAction.dynamicMergeAction = this;
	}

	@Override
	public void subscribe(Subscriber<? super O> subscriber) {
		fanInAction.subscribe(subscriber);
	}

	@Override
	protected PushSubscription<O> createSubscription(Subscriber<? super O> subscriber, boolean reactivePull) {
		throw new IllegalAccessError("Should never use dynamicMergeAction own createSubscription");
	}

	@Override
	public void requestMore(long n) {
		if(upstreamSubscription != null) upstreamSubscription.request(n);
	}

	@Override
	protected void doNext(Publisher<? extends I> ev) {
		//IGNORE
	}

	@Override
	public void onNext(Publisher<? extends I> value) {
		WIP_UPDATER.incrementAndGet(this);
		fanInAction.addPublisher(value);
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		super.onSubscribe(subscription);
		fanInAction.start();
		long toRequest = REQUESTED_UPDATER.getAndSet(this, 0l);
		if(toRequest > 0l){
			requestMore(toRequest);
		}
	}

	@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		if (upstreamSubscription != null && !terminated) {
			long toRequest = BackpressureUtils.getAndAdd(REQUESTED_UPDATER, this, elements);

			if(toRequest == 0){
				toRequest = elements;
			} else if(elements != toRequest){
				//upstreamSubscription.clearPendingRequest();
				toRequest = elements + toRequest;
			}

			if (toRequest > 0) {
				requestMore(toRequest);
			}
		} else {
			BackpressureUtils.getAndAdd(REQUESTED_UPDATER, this, elements);
		}
	}

	@Override
	public void onError(Throwable cause) {
		fanInAction.onError(cause);
	}

	@Override
	public void onComplete() {
		if(wip == 0) {
			fanInAction.scheduleCompletion();
		}
		super.onComplete();
	}

	@Override
	public Action<Publisher<? extends I>, O> capacity(long elements) {
		fanInAction.capacity(elements);
		return super.capacity(elements);
	}

	public int decrementWip(){
		return WIP_UPDATER.decrementAndGet(this);
	}

	public FanInAction<I, ?, O, ? extends FanInAction.InnerSubscriber<I, ?, O>> mergedStream() {
		return fanInAction;
	}

	@Override
	public String toString() {
		return super.toString()+"{" +
				"wip=" + wip +
				", requested=" + requested +
				'}';
	}
}
