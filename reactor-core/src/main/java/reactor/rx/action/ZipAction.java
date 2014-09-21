/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class ZipAction<O, V, TUPLE extends Tuple> extends FanInAction<O, V> {

	final Function<TUPLE, ? extends V> accumulator;
	final ArrayList<O>                 buffer;

	int index = 0;

	final Consumer<Long> upstreamConsumer = new Consumer<Long>() {
		@Override
		public void accept(Long integer) {
			try {
				tryBroadcastTuple();
				requestConsumer.accept(integer);
			} catch (Throwable e) {
				doError(e);
			}
		}
	};

	@SuppressWarnings("unchecked")
	private void tryBroadcastTuple() {
		if (currentNextSignals == capacity) {
			broadcastNext(accumulator.apply((TUPLE) Tuple.of(buffer)));
			buffer.clear();
			ensureCapacity();
		}
	}

	public ZipAction(Dispatcher dispatcher,
	                 Function<TUPLE, ? extends V> accumulator, Iterable<? extends Publisher<? extends O>>
			composables) {
		super(dispatcher, composables);
		this.accumulator = accumulator;
		this.buffer = new ArrayList<>(8);
	}

	private void ensureCapacity() {
		int size = innerSubscriptions.subscriptions.size();
		buffer.ensureCapacity(size);
		while (buffer.size() < size) {
			buffer.add(null);
		}
	}

	@Override
	protected FanInSubscription<O> createFanInSubscription() {
		return new ZipSubscription<O>(this, new ArrayList<FanInSubscription.InnerSubscription>(8));
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doNext(O ev) {
		tryBroadcastTuple();
	}

	@Override
	protected FanInAction.InnerSubscriber<O, V> createSubscriber() {
		capacity = innerSubscriptions.subscriptions.size() + 1;
		return new ZipAction.InnerSubscriber<>(this, index++);
	}


	@Override
	protected long initUpstreamPublisherAndCapacity() {
		for (Publisher<? extends O> composable : composables) {
			addPublisher(composable);
		}
		return innerSubscriptions.subscriptions.size();
	}

	@Override
	protected void onRequest(long n) {
		dispatch(n, upstreamConsumer);
	}


	@Override
	public String toString() {
		return super.toString() +
				"{staged=" + buffer.size() +
				'}';
	}

	private static class InnerSubscriber<O, V, TUPLE extends Tuple> extends FanInAction.InnerSubscriber<O, V> {
		private final int                    index;
		final         ZipAction<O, V, TUPLE> outerAction;

		InnerSubscriber(ZipAction<O, V, TUPLE> outerAction, int index) {
			super(outerAction);
			this.outerAction = outerAction;
			this.index = index;
		}

		@Override
		public void onSubscribe(Subscription subscription) {
			this.s = new FanInSubscription.InnerSubscription(subscription);

			outerAction.innerSubscriptions.addSubscription(s);
			outerAction.dispatch(new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					if (outerAction.index != outerAction.buffer.size()) {
						outerAction.ensureCapacity();
					}
					if (outerAction.innerSubscriptions.getCapacity().get() > 0) {
						s.request(1);
					}
				}
			});
		}

		@Override
		public void onNext(final O ev) {
			outerAction.dispatch(new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					if (null != outerAction.buffer.set(index, ev)) {
						outerAction.doError(new IllegalStateException("previous value still present while consuming a new signal" +
								" " +
								"from this stream :" + this.toString()));
					} else {
						outerAction.innerSubscriptions.onNext(ev);
					}
				}
			});
		}

		@Override
		public void onComplete() {
			if (outerAction.getDispatcher().inContext()) {
				s.toRemove = true;
			} else {
				outerAction.innerSubscriptions.removeSubscription(s);
			}
			outerAction.innerSubscriptions.onComplete();
		}
	}

	private static class ZipSubscription<O> extends FanInSubscription<O> {
		public ZipSubscription(Subscriber<O> subscriber, List<InnerSubscription> subs) {
			super(subscriber, subs);
		}

		@Override
		protected void parallelRequest(final long elements) {
			super.parallelRequest(1);
		}
	}
}
