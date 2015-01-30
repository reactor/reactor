/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.rx.action.control;

import org.reactivestreams.Subscriber;
import reactor.core.Dispatcher;
import reactor.fn.Consumer;
import reactor.rx.action.Action;
import reactor.rx.subscription.PushSubscription;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public final class DispatcherAction<T> extends Action<T, T> {

	private final Dispatcher dispatcher;


	public DispatcherAction(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	@Override
	public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
		return this.dispatcher != dispatcher;
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		final PushSubscription<T> subscription = createSubscription(subscriber, false);

		if (subscription == null)
			return;

		subscribeWithSubscription(subscriber, subscription, false);
	}
/*
	@Override
	public void requestMore(long n) {
		checkRequest(n);
		dispatcher.dispatch(n, upstreamSubscription, null);
	}*/


	@Override
	public void onNext(T ev) {
		if(dispatcher.inContext()){
			super.onNext(ev);
		} else {
			dispatcher.dispatch(ev, this, null);
		}
	}

	@Override
	public void onError(Throwable cause) {
		if(dispatcher.inContext()){
			super.onError(cause);
		} else {
			dispatcher.dispatch(cause, new Consumer<Throwable>() {
				@Override
				public void accept(Throwable throwable) {
					doError(throwable);
				}
			}, null);
		}
	}

	@Override
	public void onComplete() {
		if(dispatcher.inContext()){
			super.onComplete();
		} else {
			dispatcher.dispatch(null, new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					doComplete();
				}
			}, null);
		}
	}

	@Override
	public Dispatcher getDispatcher() {
		return dispatcher;
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
		if (upstreamSubscription != null
				&& upstreamSubscription.shouldRequestPendingSignals()) {

			long left = upstreamSubscription.pendingRequestSignals();
			if (left > 0l) {
				upstreamSubscription.updatePendingRequests(-left);
				dispatcher.dispatch(left, upstreamSubscription, null);
			}
		}
	}
}
