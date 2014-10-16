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
package reactor.rx.action;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.support.WrappedSubscription;

/**
 * @author Stephane Maldini
 */
public final class TerminalCallbackAction<T> extends Action<T, Void> {

	private final Consumer<? super T>         consumer;
	private final Consumer<? super Throwable> errorConsumer;
	private final Consumer<Void>              completeConsumer;

	public TerminalCallbackAction(Dispatcher dispatcher, Consumer<? super T> consumer,
	                              Consumer<? super Throwable> errorConsumer, Consumer<Void> completeConsumer) {
		super(dispatcher);
		this.consumer = consumer;
		this.errorConsumer = errorConsumer;
		this.completeConsumer = completeConsumer;
		this.capacity = Long.MAX_VALUE;
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		subscription.request(capacity);
	}

	@Override
	protected void doNext(T ev) {
		if(consumer != null){
			consumer.accept(ev);
		}
	}

	@Override
	protected PushSubscription<Void> createSubscription(Subscriber<? super Void> subscriber, boolean reactivePull) {
		return new PushSubscription<Void>(this, subscriber){
			@Override
			public void request(long n) {
				//IGNORE
			}
		};
	}

	@Override
	protected PushSubscription<T> createTrackingSubscription(final Subscription subscription) {
		return new WrappedSubscription<T>(subscription, this){

			@Override
			public void incrementCurrentNextSignals() {
				pendingRequestSignals--;
			}

			@Override
			public void doPendingRequest() {
				request(capacity);
			}

			@Override
			public boolean shouldRequestPendingSignals() {
				return pendingRequestSignals == 0;
			}

			@Override
			public String toString() {
				return super.toString()+" pending="+pendingRequestSignals;
			}
		};
	}

	@Override
	protected void doError(Throwable ev) {
		if(errorConsumer != null){
			errorConsumer.accept(ev);
		}
		super.doError(ev);
		cancel();
	}

	@Override
	protected void doComplete() {
		if(completeConsumer != null){
			completeConsumer.accept(null);
		}
		super.doComplete();
		cancel();
	}

}
