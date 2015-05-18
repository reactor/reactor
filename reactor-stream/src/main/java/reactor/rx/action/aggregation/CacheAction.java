/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.rx.action.aggregation;

import org.reactivestreams.Subscriber;
import reactor.core.queue.CompletableQueue;
import reactor.core.support.Exceptions;
import reactor.fn.Consumer;
import reactor.rx.action.Action;
import reactor.rx.action.Signal;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.ReactiveSubscription;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class CacheAction<T> extends Action<T, T> {

	private final List<Signal<T>> values = new ArrayList<>();

	@Override
	protected PushSubscription<T> createSubscription(final Subscriber<? super T> subscriber, CompletableQueue<T> queue) {
		final Consumer<Long> requestConsumer = new Consumer<Long>() {
			int cursor = 0;

			@Override
			public void accept(Long elem) {
				PushSubscription<T> upstream = null;
				synchronized (values) {
					if (values.isEmpty()) {
						upstream = upstreamSubscription;
					}
				}

				if(upstream != null) {
					upstreamSubscription.request(elem);
					return;
				}

				long toRequest = elem;
				List<Signal<T>> toSend = null;
				synchronized (values) {
					if (cursor < values.size()) {
						toSend = elem == Long.MAX_VALUE ? new ArrayList<>(values) :
								values.subList(cursor, Math.max(cursor + elem.intValue(), values.size()));
					}
				}

				if(toSend != null) {

					for (Signal<T> signal : toSend) {
						cursor++;
						if (signal.isOnNext()) {
							subscriber.onNext(signal.get());
						} else if (signal.isOnComplete()) {
							subscriber.onComplete();
							break;
						} else if (signal.isOnError()) {
							subscriber.onError(signal.getThrowable());
							break;
						}
					}
					toRequest = elem == Long.MAX_VALUE ? elem : elem - toSend.size();
				}


				if (toRequest > 0 && upstreamSubscription != null) {
						upstreamSubscription.request(toRequest);
					}
			}
		};

		if (queue != null) {
			return new ReactiveSubscription<T>(this, subscriber, queue) {

				@Override
				protected void onRequest(long elements) {
					requestConsumer.accept(elements);
					if (upstreamSubscription == null) {
						updatePendingRequests(elements);
					}
				}
			};
		} else {
			return new PushSubscription<T>(this, subscriber) {
				@Override
				protected void onRequest(long elements) {
					requestConsumer.accept(elements);
					if (upstreamSubscription == null) {
						updatePendingRequests(elements);
					}
				}
			};
		}
	}

	@Override
	protected void subscribeWithSubscription(Subscriber<? super T> subscriber, PushSubscription<T> subscription) {
		try {
			if (!addSubscription(subscription)) {
				subscriber.onError(new IllegalStateException("The subscription cannot be linked to this Stream"));
			} else {
				subscriber.onSubscribe(subscription);
			}
		} catch (Exception e) {
			Exceptions.throwIfFatal(e);
			subscriber.onError(e);
		}
	}

	@Override
	protected void doComplete() {
		synchronized (values) {
			values.add(Signal.<T>complete());
		}
		super.doComplete();
	}

	@Override
	protected void doError(Throwable ev) {
		synchronized (values){
			values.add(Signal.<T>error(ev));
		}
		super.doError(ev);
	}

	@Override
	public void doNext(T value) {
		synchronized (values) {
			values.add(Signal.next(value));
		}
		broadcastNext(value);
	}
}
