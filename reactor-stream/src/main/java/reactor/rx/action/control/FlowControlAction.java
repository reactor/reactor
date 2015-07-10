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
package reactor.rx.action.control;

import org.reactivestreams.Subscriber;
import reactor.ReactorProcessor;
import reactor.core.queue.CompletableQueue;
import reactor.fn.Supplier;
import reactor.rx.action.Action;
import reactor.rx.subscription.DropSubscription;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.ReactiveSubscription;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class FlowControlAction<O> extends Action<O, O> {

	private final Supplier<? extends CompletableQueue<O>> queueSupplier;

	public FlowControlAction(Supplier<? extends CompletableQueue<O>> queueSupplier) {
		this.queueSupplier = queueSupplier;
	}

	@Override
	protected void doNext(O ev) {
		broadcastNext(ev);
	}

	@Override
	public void onError(Throwable cause) {
		doError(cause);
	}

	@Override
	public boolean isReactivePull(ReactorProcessor dispatcher, long producerCapacity) {
		return false;
	}

	@Override
	protected PushSubscription<O> createSubscription(Subscriber<? super O> subscriber, boolean reactivePull) {
		if (queueSupplier != null) {
			return new ReactiveSubscription<O>(this, subscriber, queueSupplier.get()) {
				@Override
				public void onRequest(long elements) {
					super.onRequest(elements);
					requestUpstream(capacity, buffer.isComplete(), capacity);
				}
			};
		} else {
			return new DropSubscription<O>(this, subscriber){
				@Override
				public void request(long elements) {
					super.request(elements);
					requestUpstream(capacity, isComplete(), capacity);
				}
			};
		}
	}
}
