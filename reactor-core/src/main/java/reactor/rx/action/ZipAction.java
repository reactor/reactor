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
public class ZipAction<O, V, TUPLE extends Tuple> extends FanInAction<O, V, ZipAction.InnerSubscriber<O, V>> {

	final Function<TUPLE, ? extends V> accumulator;

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

	public ZipAction(Dispatcher dispatcher,
	                 Function<TUPLE, ? extends V> accumulator, Iterable<? extends Publisher<? extends O>>
			composables) {
		super(dispatcher, composables);
		this.accumulator = accumulator;
	}

	@SuppressWarnings("unchecked")
	private void tryBroadcastTuple() {
		if (currentNextSignals == capacity) {
			final Object[] result = new Object[innerSubscriptions.subscriptions.size()];

			innerSubscriptions.forEach(
					new Consumer<FanInSubscription.InnerSubscription<O, ? extends InnerSubscriber<O, V>>>() {
						@Override
						public void accept(FanInSubscription.InnerSubscription<O, ? extends InnerSubscriber<O, V>> subscription) {
							result[subscription.subscriber.index] = subscription.subscriber.lastItem;
						}
					});

			downstreamSubscription().onNext(accumulator.apply((TUPLE) Tuple.of((Object[]) result)));

			if (innerSubscriptions.getBuffer().isComplete()) {
				innerSubscriptions.cancel();
				broadcastComplete();
			}
		}
	}


	@Override
	protected FanInSubscription<O, InnerSubscriber<O, V>> createFanInSubscription() {
		return new ZipSubscription<O, V>(this,
				new ArrayList<FanInSubscription.InnerSubscription<O, ? extends ZipAction.InnerSubscriber<O, V>>>(8));
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doNext(O ev) {
		tryBroadcastTuple();
	}


	@Override
	protected InnerSubscriber<O, V> createSubscriber() {
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

	public static class InnerSubscriber<O, V> extends FanInAction.InnerSubscriber<O, V> {
		final FanInAction<O, V, InnerSubscriber<O, V>> outerAction;
		final int                                      index;

		O lastItem;

		InnerSubscriber(FanInAction<O, V, InnerSubscriber<O, V>> outerAction, int index) {
			super(outerAction);
			this.index = index;
			this.outerAction = outerAction;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onSubscribe(Subscription subscription) {
			setSubscription(new FanInSubscription.InnerSubscription<O, InnerSubscriber<O, V>>(subscription, this));

			outerAction.innerSubscriptions.addSubscription(s);
			if (outerAction.innerSubscriptions.getCapacity().get() > 0) {
				s.request(1);
			}
		}

		@Override
		public void onNext(O ev) {
			lastItem = ev;
			super.onNext(ev);
		}

		@Override
		public void onComplete() {
			//Action.log.debug("event [complete] by: " + this);
			if (checkDynamicMerge()) {
				outerAction.innerSubscriptions.getBuffer().complete();
			}
		}

		@Override
		public String toString() {
			return "ZipAction.InnerSubscriber";
		}
	}

	private static class ZipSubscription<O, V> extends FanInSubscription<O, ZipAction.InnerSubscriber<O, V>> {
		public ZipSubscription(Subscriber<O> subscriber,
		                       List<InnerSubscription<O, ? extends ZipAction.InnerSubscriber<O, V>>> subs) {
			super(subscriber, subs);
		}

		@Override
		protected void parallelRequest(final long elements) {
			super.parallelRequest(1);
		}
	}
}
