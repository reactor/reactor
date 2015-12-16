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
package reactor.rx.action.aggregation;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.support.BackpressureUtils;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public final class LastOperator<T> implements Publishers.Operator<T, T> {

	public static final LastOperator INSTANCE = new LastOperator();

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new LastAction<>(subscriber);
	}

	static final class LastAction<T> extends SubscriberWithDemand<T, T> {

		private T last;

		private final AtomicLongFieldUpdater<LastAction> COUNTED = AtomicLongFieldUpdater.newUpdater(LastAction
				.class, "count");

		private volatile long count;

		public LastAction(Subscriber<? super T> subscriber) {
			super(subscriber);
		}

		@Override
		protected void doNext(T value) {
			last = value;
			if(requestedFromDownstream() != Long.MAX_VALUE && COUNTED.decrementAndGet(this) == 0L){
				requestMore(count = requestedFromDownstream());
			}
		}

		@Override
		protected void doRequested(long before, long n) {
			BackpressureUtils.getAndAdd(COUNTED, this, n);
			if(before == 0) {
				requestMore(n);
			}

		}

		@Override
		protected void checkedComplete() {
			if (last != null) {
				subscriber.onNext(last);
			}

			subscriber.onComplete();
		}
	}
}
