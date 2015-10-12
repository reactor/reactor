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
package reactor.rx.action.metrics;

import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.rx.action.Action;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class CountAction<T> extends Action<T, Long> {

	private final AtomicLong counter = new AtomicLong(-1);

	public CountAction() {
	}

	@Override
	protected void doOnSubscribe(Subscription subscription) {
		counter.compareAndSet(-1, 0);
	}

	@Override
	public void subscribe(final Subscriber<? super Long> subscriber) {
		Subscription sub = upstreamSubscription;
		if (sub == null && counter.get() != -1) {
			subscriber.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					subscriber.onNext(counter.get());
					subscriber.onComplete();
				}

				@Override
				public void cancel() {
				}
			});
		} else {
			super.subscribe(subscriber);

		}
	}

	@Override
	protected void doNext(T value) {
		broadcastNext(counter.incrementAndGet());
	}
}
