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
import reactor.core.Dispatcher;
import reactor.core.queue.CompletableQueue;
import reactor.fn.Consumer;
import reactor.fn.timer.Timer;
import reactor.rx.action.Signal;
import reactor.rx.action.Action;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.ReactiveSubscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class CacheAction<T> extends Action<T, T> {

	private final List<Signal<T>> values = new ArrayList<>();

	public CacheAction(Dispatcher dispatcher, int batchsize) {
		this(dispatcher, batchsize, -1, null, null);
	}

	public CacheAction(Dispatcher dispatcher, int maxSize, long timespan, TimeUnit unit, Timer timer) {
		super(dispatcher);

	}

	@Override
	protected PushSubscription<T> createSubscription(final Subscriber<? super T> subscriber, CompletableQueue<T> queue) {
		final Consumer<Long> requestConsumer = new Consumer<Long>() {
			int cursor = 0;

			@Override
			public void accept(Long elem) {
				if (values.isEmpty()) {
					if(upstreamSubscription != null) {
						upstreamSubscription.accept(elem);
					}
					return;
				}

				long toRequest = elem;
				if(cursor < values.size()) {
					List<Signal<T>> toSend = elem == Long.MAX_VALUE ? new ArrayList<>(values) :
							values.subList(cursor, Math.max(cursor + elem.intValue(), values.size()));

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
						upstreamSubscription.accept(toRequest);
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
	protected void doComplete() {
		values.add(Signal.<T>complete());
		super.doComplete();
	}

	@Override
	protected void doError(Throwable ev) {
		values.add(Signal.<T>error(ev));
		super.doError(ev);
	}

	@Override
	public void doNext(T value) {
		values.add(Signal.next(value));
		broadcastNext(value);
	}
}
