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

package reactor.core.publisher.operator;

import org.reactivestreams.Subscriber;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.support.Assert;
import reactor.fn.Function;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;

/**
 * @author Stephane Maldini
 * @since 1.1, 2.0, 2.1
 */
public final class MapOperator<T, V> implements Function<Subscriber<? super V>, Subscriber<? super T>> {

	/**
	 * A predefined map operator producing timestamp tuples
	 */
	private static final MapOperator TIMESTAMP_OPERATOR =
			new MapOperator<>(new Function<Object, Tuple2<Long, ?>>() {
				@Override
				public Tuple2<Long, ?> apply(Object o) {
					return Tuple.of(System.currentTimeMillis(), o);
				}
			});

	@SuppressWarnings("unchecked")
	public static <T> MapOperator<T, Tuple2<Long, T>> timestamp(){
		return (MapOperator<T, Tuple2<Long, T>> )TIMESTAMP_OPERATOR;
	}

	private final Function<? super T, ? extends V> fn;

	public MapOperator(Function<? super T, ? extends V> fn) {
		this.fn = fn;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super V> subscriber) {
		return new MapBarrier<>(subscriber, fn);
	}

	static final class MapBarrier<T, V> extends SubscriberBarrier<T, V> {

		private final Function<? super T, ? extends V> fn;

		public MapBarrier(Subscriber<? super V> actual, Function<? super T, ? extends V> fn) {
			super(actual);
			Assert.notNull(fn, "Map function cannot be null.");
			this.fn = fn;
		}

		@Override
		protected void doNext(T value) {
			V res = fn.apply(value);
			if (res != null) {
				subscriber.onNext(res);
			}
		}

	}

}
