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

package reactor.rx.stream;

import java.util.Map;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.Publishers;
import reactor.fn.BiFunction;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class StreamReduceByKey<K, V>
		implements Publishers.Operator<Tuple2<K, V>, Tuple2<K, V>> {

	private final BiFunction<? super V, ? super V, V>        fn;
	private final Map<K, V>                                  store;
	private final Publisher<? extends StreamKv.Signal<K, V>> mapListener;

	public StreamReduceByKey(BiFunction<? super V, ? super V, V> fn,
			Map<K, V> store,
			Publisher<? extends StreamKv.Signal<K, V>> mapListener) {
		this.fn = fn;
		this.store = store;
		this.mapListener = mapListener;
	}

	@Override
	public Subscriber<? super Tuple2<K, V>> apply(Subscriber<? super Tuple2<K, V>> subscriber) {
		return new ReduceByKeyAction<>(subscriber, fn, store, mapListener);
	}

	static final class ReduceByKeyAction<K, V> extends StreamScanByKey.ScanByKeyAction<K, V> {

		public ReduceByKeyAction(Subscriber<? super Tuple2<K, V>> actual,
				BiFunction<? super V, ? super V, V> fn,
				Map<K, V> store,
				Publisher<? extends StreamKv.Signal<K, V>> mapListener) {
			super(actual, fn, store, mapListener);
		}

		@Override
		protected void performNext(Tuple2<K, V> ev) {
			//IGNORE
		}

		@Override
		protected void doComplete() {
			if (store.isEmpty()) {
				return;
			}

			for (Map.Entry<K, V> entry : store.entrySet()) {
				subscriber.onNext(Tuple.of(entry.getKey(), entry.getValue()));
			}
			subscriber.onComplete();
		}
	}

}
