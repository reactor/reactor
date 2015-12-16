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

package reactor.rx.action.filter;

import org.reactivestreams.Subscriber;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberWithDemand;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class TakeOperator<T> implements Publishers.Operator<T, T> {

	private final long limit;

	public TakeOperator(long limit) {
		this.limit = limit;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new TakeAction<>(subscriber, limit);
	}

	static final class TakeAction<T> extends SubscriberWithDemand<T, T> {

		private final long limit;

		private long counted = 0L;

		public TakeAction(Subscriber<? super T> actual, long limit) {
			super(actual);
			this.limit = limit;
		}

		@Override
		protected void doRequest(long n) {
			if (n >= limit) {
				requestMore(limit);
			}
			else {
				long r, toAdd;
				do {
					r = requestedFromDownstream();
					if (r >= limit) {
						return;
					}
					toAdd = r + n;
				}
				while (!REQUESTED.compareAndSet(this, r, toAdd));

				if (toAdd >= limit) {
					requestMore(limit - r);
				}
				else {
					requestMore(n);
				}
			}
		}

		@Override
		protected void doNext(T ev) {
			subscriber.onNext(ev);

			if (++counted >= limit) {
				cancel();
				subscriber.onComplete();
			}
		}

		@Override
		public long getCapacity() {
			return limit;
		}

		@Override
		public String toString() {
			return super.toString() + "{" +
					"take=" + limit +
					", counted=" + counted +
					'}';
		}
	}
}
