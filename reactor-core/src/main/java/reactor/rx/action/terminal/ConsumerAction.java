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
package reactor.rx.action.terminal;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.fn.Consumer;
import reactor.rx.action.Action;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.support.WrappedSubscription;

/**
 * @author Stephane Maldini
 */
public final class ConsumerAction<T> extends Action<T, Void> {

	private final Consumer<? super T>         consumer;
	private final Consumer<? super Throwable> errorConsumer;
	private final Consumer<Void>              completeConsumer;

	public ConsumerAction(Dispatcher dispatcher, Consumer<? super T> consumer,
	                      Consumer<? super Throwable> errorConsumer, Consumer<Void> completeConsumer) {
		super(dispatcher);
		this.consumer = consumer;
		this.errorConsumer = errorConsumer;
		this.completeConsumer = completeConsumer;
		//TODO define option to choose ?
		this.capacity = Long.MAX_VALUE;
	}

	@Override
	protected void doNext(T ev) {
		if (consumer != null) {
			consumer.accept(ev);
		}
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		upstreamSubscription = createTrackingSubscription(subscription);
	}

	@Override
	public void requestMore(long n) {
		if (upstreamSubscription != null) {
			dispatch(n, upstreamSubscription);
		}
	}

	@Override
	protected PushSubscription<Void> createSubscription(Subscriber<? super Void> subscriber, boolean reactivePull) {
		return new PushSubscription<Void>(this, subscriber) {
			@Override
			public void request(long n) {
				//IGNORE
			}
		};
	}

	@Override
	protected void doStart(long pending) {
		//IGNORE
	}

	@Override
	protected PushSubscription<T> createTrackingSubscription(final Subscription subscription) {
		if (capacity != Long.MAX_VALUE) {
			return new WrappedSubscription<T>(subscription, this) {

				@Override
				public void updatePendingRequests(long n) {
					synchronized (this) {
						pendingRequestSignals = 0l;
					}
				}

				@Override
				public boolean shouldRequestPendingSignals() {
					return consumer != null && --pendingRequestSignals == 0l && PENDING_UPDATER.compareAndSet(this, 0l,
							capacity);
				}

				@Override
				public String toString() {
					return super.toString() + " pending=" + pendingRequestSignals();
				}
			};
		} else {
			return super.createTrackingSubscription(subscription);
		}
	}

	@Override
	protected void doError(Throwable ev) {
		if (errorConsumer != null) {
			errorConsumer.accept(ev);
		}
		super.doError(ev);
	}

	@Override
	protected void doComplete() {
		if (completeConsumer != null) {
			completeConsumer.accept(null);
		}
		super.doComplete();
	}

}
