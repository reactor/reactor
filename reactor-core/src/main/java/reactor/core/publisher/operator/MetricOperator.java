/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Tersion 2.0 (the "License");
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

package reactor.core.publisher.operator;

import org.reactivestreams.Subscriber;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.support.Assert;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.fn.Function;

/**
 * @author Stephane Maldini
 * @since 1.1, 2.0, 2.1
 */
public final class MetricOperator<T> implements Function<Subscriber<? super T>, Subscriber<? super T>> {
	
	private final Function<? super T, ? extends T> fn;

	public MetricOperator(Function<? super T, ? extends T> fn) {
		this.fn = fn;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new MetricBarrier<>(subscriber, fn);
	}

	static final class MetricBarrier<T> extends SubscriberBarrier<T, T> implements ReactiveState.Named {

		private final Function<? super T, ? extends T> fn;

		public MetricBarrier(Subscriber<? super T> actual, Function<? super T, ? extends T> fn) {
			super(actual);
			Assert.notNull(fn, "Map function cannot be null.");
			this.fn = fn;
		}

		@Override
		public String getName() {
			return ReactiveStateUtils.getName(fn);
		}

		@Override
		protected void doNext(T value) {
			T res = fn.apply(value);
			if (res != null) {
				subscriber.onNext(res);
			}
		}

	}

}
