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
package reactor.rx.subscription.support;

import reactor.queue.CompletableQueue;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.ReactiveSubscription;

/**
* @author Stephane Maldini
*/
public final class WrappedPushToReactiveSubscription<O> extends ReactiveSubscription<O> {
	final PushSubscription<O> thiz;

	public WrappedPushToReactiveSubscription(final PushSubscription<O> thiz, CompletableQueue<O> queue) {
		super(thiz.getPublisher(), new SubscriberToPushSubscription<O>(thiz), queue);
		this.thiz = thiz;
	}

	@Override
	public void onRequest(long elements) {
		super.onRequest(elements);
		thiz.request(elements);
	}

	@Override
	public void cancel() {
		super.cancel();
		thiz.cancel();
	}

	@Override
	public void doPendingRequest() {
		super.doPendingRequest();
		thiz.doPendingRequest();
	}

	@Override
	public String toString() {
		return super.toString()+" wrapped="+thiz+" ";
	}

	@Override
	public boolean equals(Object o) {
		return !(o == null || thiz.getClass() != o.getClass()) && thiz.equals(o);
	}

	@Override
	public int hashCode() {
		return thiz.hashCode();
	}
}
