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

package reactor.core.subscriber;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.support.BackpressureUtils;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public class SubscriberWithDemand<I, O> extends SubscriberBarrier<I, O> {

	@SuppressWarnings("unused")
	private volatile       int                                             terminated = 0;
	@SuppressWarnings("rawtypes")
	protected static final AtomicIntegerFieldUpdater<SubscriberWithDemand> TERMINATED =
			AtomicIntegerFieldUpdater.newUpdater(SubscriberWithDemand.class, "terminated");

	@SuppressWarnings("unused")
	private volatile       long                                         requested = 0L;
	@SuppressWarnings("rawtypes")
	protected static final AtomicLongFieldUpdater<SubscriberWithDemand> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(SubscriberWithDemand.class, "requested");

	public SubscriberWithDemand(Subscriber<? super O> subscriber) {
		super(subscriber);
	}

	/**
	 *
	 * @return
	 */
	public final boolean isTerminated() {
		return terminated == 1;
	}

	/**
	 *
	 * @return
	 */
	public final long getRequested() {
		return requested;
	}

	@Override
	protected void doRequest(long n) {
		doRequested(BackpressureUtils.getAndAdd(REQUESTED, this, n), n);
	}

	protected void doRequested(long before, long n){
		requestMore(n);
	}

	protected final void requestMore(long n){
		Subscription s = this.subscription;
		if (s != null) {
			s.request(n);
		}
	}

	@Override
	protected void doComplete() {
		if (TERMINATED.compareAndSet(this, 0, 1)) {
			checkedComplete();
			doTerminate();
		}
	}

	protected void checkedComplete(){
		subscriber.onComplete();
	}

	@Override
	protected void doError(Throwable throwable) {
		if (TERMINATED.compareAndSet(this, 0, 1)) {
			checkedError(throwable);
			doTerminate();
		}
	}

	protected void checkedError(Throwable throwable){
		subscriber.onError(throwable);
	}

	@Override
	protected void doCancel() {
		if (TERMINATED.compareAndSet(this, 0, 1)) {
			checkedCancel();
			doTerminate();
		}
	}

	protected void checkedCancel(){
		super.doCancel();
	}

	protected void doTerminate(){
		//TBD
	}

	@Override
	public String toString() {
		return super.toString() + (requested != 0L ? "{requested=" + requested + "}": "");
	}
}
