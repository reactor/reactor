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
package reactor.rx.action;

import java.util.HashSet;
import java.util.Set;

import org.reactivestreams.Subscriber;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Function;

/**
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class DistinctOperator<T, V> implements Publishers.Operator<T, T> {

	private final Function<? super T, ? extends V> keySelector;

	public DistinctOperator(Function<? super T, ? extends V> keySelector) {
		this.keySelector = keySelector;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new DistinctAction<>(subscriber, keySelector);
	}

	static final class DistinctAction<T, V> extends SubscriberBarrier<T, T> {

		private final Set<V> keySet = new HashSet<V>();

		private final Function<? super T, ? extends V> keySelector;

		public DistinctAction(Subscriber<? super T> actual, Function<? super T, ? extends V> keySelector) {
			super(actual);
			this.keySelector = keySelector;
		}

		@Override
		@SuppressWarnings("unchecked")
		protected void doNext(T currentData) {
			V currentKey;
			if (keySelector != null) {
				currentKey = keySelector.apply(currentData);
			} else {
				currentKey = (V) currentData;
			}

			if(keySet.add(currentKey)) {
				subscriber.onNext(currentData);
			}
		}

		@Override
		protected void doComplete() {
			subscriber.onComplete();
			keySet.clear();
		}

	}


}
