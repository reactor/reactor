/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.rx.action.pair;

import org.reactivestreams.Publisher;
import reactor.fn.BiFunction;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.rx.stream.MapStream;
import reactor.rx.subscription.PushSubscription;

import java.util.Map;

/**
 * @author Stephane Maldini
 * @since 1.1, 2.0
 */
public class ReduceByKeyAction<K, V> extends ScanByKeyAction<K, V> {

	public ReduceByKeyAction(BiFunction<? super V, ? super V, V> fn, MapStream<K, V> mapStream) {
		super(fn, mapStream);
	}

	public ReduceByKeyAction(BiFunction<? super V, ? super V, V> fn, Map<K, V> store, Publisher<? extends MapStream
			.Signal<K, V>> mapListener) {
		super(fn, store, mapListener);
	}

	protected void doNext(PushSubscription<Tuple2<K, V>> subscriber, Tuple2<K, V> ev) {
		//IGNORE
	}

	@Override
	protected void doComplete() {
		if(store.isEmpty()) return;

		for(Map.Entry<K,V> entry : store.entrySet()){
			broadcastNext(Tuple.of(entry.getKey(), entry.getValue()));
		}
		broadcastComplete();
	}
}
