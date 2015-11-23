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
import reactor.Publishers;
import reactor.core.subscriber.SubscriberBarrier;

/**
 * @author Stephane Maldini
 * @since 1.1, 2.0, 2.1
 */
public final class CountOperator<T> implements Publishers.Operator<T, Long> {

	public static final CountOperator INSTANCE = new CountOperator<>();

	@Override
	public Subscriber<? super T> apply(Subscriber<? super Long> subscriber) {
		return new CountAction<>(subscriber);
	}

	final static class CountAction<T> extends SubscriberBarrier<T, Long> {

		private final AtomicLong counter = new AtomicLong(-1L);

		public CountAction(Subscriber<? super Long> actual) {
			super(actual);
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			counter.compareAndSet(-1, 0);
			subscriber.onSubscribe(this);
		}

		@Override
		protected void doNext(T value) {
			subscriber.onNext(counter.incrementAndGet());
		}
	}
}
