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

import java.util.HashMap;
import java.util.Map;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.BiFunction;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.5
 */
public final class StreamScanByKey<K, V>
		extends StreamBarrier<Tuple2<K, V>, Tuple2<K, V>> {

	protected final BiFunction<? super V, ? super V, V>        fn;
	protected final Publisher<? extends StreamKv.Signal<K, V>> mapListener;
	protected final Map<K, V>                                  store;

	public StreamScanByKey(Publisher<Tuple2<K, V>> source, BiFunction<? super V, ? super V, V> fn,
			Publisher<? extends StreamKv.Signal<K, V>> mapListener,
			Map<K, V> store) {
		super(source);
		this.fn = fn;
		this.mapListener = mapListener;
		this.store = store;
	}

	@Override
	public Subscriber<? super Tuple2<K, V>> apply(Subscriber<? super Tuple2<K, V>> subscriber) {
		return new ScanByKeyAction<>(subscriber, fn, store, mapListener);
	}

	static class ScanByKeyAction<K, V> extends SubscriberBarrier<Tuple2<K, V>, Tuple2<K, V>> {

		protected final BiFunction<? super V, ? super V, V>        fn;
		protected final Publisher<? extends StreamKv.Signal<K, V>> mapListener;
		protected final Map<K, V>                                  store;

		@SuppressWarnings("unchecked")
		public ScanByKeyAction(Subscriber<? super Tuple2<K, V>> actual,
				BiFunction<? super V, ? super V, V> fn,
				Map<K, V> store,
				Publisher<? extends StreamKv.Signal<K, V>> mapListener) {
			super(actual);
			this.fn = fn;
			this.store = store == null ? new HashMap<K, V>() : store;
			if (mapListener == null) {
				StreamKv<K, V> mapStream = null;
				if (StreamKv.class.isAssignableFrom(this.store.getClass())) {
					try {
						mapStream = (StreamKv<K, V>) this.store;
					}
					catch (ClassCastException cce) {
						//IGNORE
					}
				}
				this.mapListener = mapStream;
			}
			else {
				this.mapListener = mapListener;
			}
		}

		@Override
		protected void doOnSubscribe(final Subscription subscription) {
			if (mapListener != null) {
				mapListener.subscribe(new Subscriber<StreamKv.Signal<K, V>>() {
					Subscription s;

					@Override
					public void onSubscribe(final Subscription sub) {
						this.s = sub;
						subscriber.onSubscribe(s);
					}

					@Override
					public void onNext(StreamKv.Signal<K, V> kvSignal) {
						if (s != null && kvSignal.op() == StreamKv.Operation.put) {
							performNext(kvSignal.pair());
						}
					}

					@Override
					public void onError(Throwable t) {
						if (s != null) {
							s = null;
							subscriber.onError(t);
						}
					}

					@Override
					public void onComplete() {
						if (s != null) {
							s = null;
							subscriber.onComplete();
						}
					}
				});
			}
			else {
				subscriber.onSubscribe(this);
			}
		}

		@Override
		protected void doNext(Tuple2<K, V> ev) {
			V previous = store.get(ev.t1);
			V acc = previous == null ? ev.t2 : fn.apply(previous, ev.t2);
			store.put(ev.t1, acc);
			if (mapListener == null) {
				performNext(Tuple.of(ev.t1, acc));
			}
		}

		protected void performNext(Tuple2<K, V> ev) {
			try {
				subscriber.onNext(ev);
			}
			catch (Throwable t) {
				subscriber.onError(t);
			}
		}
	}
}
