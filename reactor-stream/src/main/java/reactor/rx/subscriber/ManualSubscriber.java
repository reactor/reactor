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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscription;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.fn.Consumer;
import reactor.rx.action.DemandControl;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class ManualSubscriber<T> extends InterruptableSubscriber<T> implements ReactiveState.Bounded, DemandControl {

	@SuppressWarnings("unused")
	private volatile long requested;
	private final AtomicLongFieldUpdater<ManualSubscriber> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(ManualSubscriber.class, "requested");

	@SuppressWarnings("unused")
	private volatile long outstanding;
	private final AtomicLongFieldUpdater<ManualSubscriber> OUTSTANDING =
			AtomicLongFieldUpdater.newUpdater(ManualSubscriber.class, "outstanding");

	@SuppressWarnings("unused")
	private volatile int running;
	private final AtomicIntegerFieldUpdater<ManualSubscriber> RUNNING =
			AtomicIntegerFieldUpdater.newUpdater(ManualSubscriber.class, "running");

	public ManualSubscriber(Consumer<? super T> consumer,
			Consumer<? super Throwable> errorConsumer,
			Consumer<Void> completeConsumer) {
		super(consumer, errorConsumer, completeConsumer);
	}

	@Override
	protected void doPostNext(T ev) {
		if(BackpressureUtils.getAndSub(OUTSTANDING, this, 1L) == 1L){
			drain();
		}
	}

	@Override
	protected void doSafeSubscribe(Subscription subscription) {
		drain();
	}

	@Override
	public void requestAll() {
		request(Long.MAX_VALUE);
	}

	@Override
	public void request(long n) {
		BackpressureUtils.checkRequest(n);
		BackpressureUtils.getAndAdd(REQUESTED, this, n);
		if(isStarted()) {
			drain();
		}
	}

	protected void drain(){
		if(RUNNING.getAndIncrement(this) == 0) {
			int missed = 1;
			long r;
			for(;;){
				if(isTerminated()){
					return;
				}
				r = REQUESTED.getAndSet(this, 0L);
				if(r > 0L) {
					BackpressureUtils.getAndAdd(OUTSTANDING, this, r);
					requestMore(r);
				}

				missed = RUNNING.addAndGet(this, -missed);
				if(missed == 0){
					break;
				}
			}
		}
	}

	@Override
	public long getCapacity() {
		return requested;
	}

	@Override
	public String toString() {
		return super.toString() + "{pending=" + requested + "}";
	}
}
