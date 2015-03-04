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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Environment;
import reactor.fn.BiFunction;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.rx.action.Action;
import reactor.rx.stream.MapStream;
import reactor.rx.subscription.PushSubscription;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Stephane Maldini
 * @since 1.1, 2.0
 */
public class ScanByKeyAction<K, V> extends Action<Tuple2<K, V>, Tuple2<K, V>> {

	protected final BiFunction<? super V, ? super V, V>         fn;
	protected final Publisher<? extends MapStream.Signal<K, V>> mapListener;
	protected final Map<K, V>                                   store;

	public ScanByKeyAction(BiFunction<? super V, ? super V, V> fn, MapStream<K, V> mapStream) {
		this(fn, mapStream, mapStream);
	}

	@SuppressWarnings("unchecked")
	public ScanByKeyAction(BiFunction<? super V, ? super V, V> fn, Map<K, V> store, Publisher<? extends MapStream
			.Signal<K, V>> mapListener) {
		this.fn = fn;
		this.store = store == null ? new HashMap<K, V>() : store;
		if (mapListener == null) {
			MapStream<K, V> mapStream = null;
			if (MapStream.class.isAssignableFrom(this.store.getClass())) {
				try {
					mapStream = (MapStream<K, V>) this.store;
				} catch (ClassCastException cce) {
					if (Environment.alive()) {
						Environment.get().routeError(cce);
					}
				}
			}
			this.mapListener = mapStream;
		} else {
			this.mapListener = mapListener;
		}
	}

	@Override
	public void subscribe(final Subscriber<? super Tuple2<K, V>> subscriber) {
		if (mapListener != null) {
			mapListener.subscribe(new Subscriber<MapStream.Signal<K, V>>() {
				Subscription s;
				PushSubscription<Tuple2<K, V>> child;

				@Override
				public void onSubscribe(final Subscription s) {
					this.s = s;
					child = new PushSubscription<Tuple2<K, V>>(ScanByKeyAction.this, subscriber) {
						@Override
						protected void onRequest(long elements) {
							s.request(elements);
							if (upstreamSubscription == null) {
								updatePendingRequests(elements);
							} else {
								requestUpstream(NO_CAPACITY, isComplete(), elements);
							}
						}

						@Override
						public void cancel() {
							super.cancel();
							s.cancel();
						}
					};

					addSubscription(child);
					subscriber.onSubscribe(child);
				}

				@Override
				public void onNext(MapStream.Signal<K, V> kvSignal) {
					if (child != null && kvSignal.op() == MapStream.Operation.put) {
						doNext(child, kvSignal.pair());
					}
				}

				@Override
				public void onError(Throwable t) {
					if (s != null) {
						s.cancel();
					}
					if (child != null) {
						child.onError(t);
					}
				}

				@Override
				public void onComplete() {
					if (s != null) {
						s.cancel();
					}
					if (child != null) {
						child.onComplete();
					}
				}
			});
		} else {
			super.subscribe(subscriber);
		}
	}

	@Override
	protected void doNext(Tuple2<K, V> ev) {
		V previous = store.get(ev.t1);
		V acc = previous == null ? ev.t2 : fn.apply(previous, ev.t2);
		store.put(ev.t1, acc);
		if (mapListener == null && downstreamSubscription != null) {
			doNext(downstreamSubscription, Tuple.of(ev.t1, acc));
		}
	}

	protected void doNext(PushSubscription<Tuple2<K, V>> subscriber, Tuple2<K, V> ev) {
		try {
			subscriber.onNext(ev);
		} catch (Throwable t) {
			subscriber.onError(t);
		}
	}
}
