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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class ZipAction<O, V, TUPLE extends Tuple> extends FanInAction<O, V, ZipAction.InnerSubscriber<O, V>> {

	final Function<TUPLE, ? extends V> accumulator;

	int index = 0;

	AtomicBoolean completing = new AtomicBoolean();

	final Consumer<Long> upstreamConsumer = new Consumer<Long>() {
		@Override
		public void accept(Long integer) {
			try {
				if (currentNextSignals == capacity) {
					broadcastTuple(false);
				}

				requestConsumer.accept(integer);
			} catch (Throwable e) {
				doError(e);
			}
		}
	};

	@SuppressWarnings("unchecked")
	public static <TUPLE extends Tuple, V> Function<TUPLE, List<V>> joinZipper() {
		return new Function<TUPLE, List<V>>() {
			@Override
			public List<V> apply(TUPLE ts) {
				return Arrays.asList((V[]) ts.toArray());
			}
		};
	}

	public ZipAction(Dispatcher dispatcher,
	                 Function<TUPLE, ? extends V> accumulator, Iterable<? extends Publisher<? extends O>>
			composables) {
		super(dispatcher, composables);
		this.accumulator = accumulator;
	}

	@SuppressWarnings("unchecked")
	protected void broadcastTuple(final boolean force) {
		final Object[] result = new Object[innerSubscriptions.subscriptions.size()];
		final AtomicInteger counter = new AtomicInteger(0);

		innerSubscriptions.forEach(
				new Consumer<FanInSubscription.InnerSubscription<O, ? extends InnerSubscriber<O, V>>>() {
					@Override
					public void accept(FanInSubscription.InnerSubscription<O, ? extends InnerSubscriber<O, V>> subscription) {
						if (subscription.subscriber.lastItem == null) {
							return;
						}
						result[subscription.subscriber.index] = subscription.subscriber.lastItem;
						subscription.subscriber.lastItem = null;
						counter.incrementAndGet();
					}
				});

		if (force || counter.get() == result.length) {
			broadcastNext(accumulator.apply((TUPLE) Tuple.of(result)));
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
		if (currentNextSignals == capacity) {
			broadcastTuple(false);
		}
	}

	@Override
	protected void doComplete() {
		//can receive multiple queued complete signals
		if (finalState == null && runningComposables.get() == 0) {
			innerSubscriptions.scheduleTermination();
			broadcastComplete();
		}
	}

	@Override
	protected void requestUpstream(AtomicLong capacity, boolean terminated, long elements) {
		super.requestUpstream(capacity, terminated, Math.max(elements, runningComposables.get()));
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
		final ZipAction<O, V, ?> outerAction;
		final int                index;

		O lastItem;

		InnerSubscriber(ZipAction<O, V, ?> outerAction, int index) {
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
				request(1);
			}
		}

		@Override
		public void request(long n) {
			pendingRequests += n - 1;
			super.request(1);
		}

		@Override
		public void onNext(O ev) {
			//Action.log.debug("event ["+ev+"] by: " + this);
			lastItem = ev;
			outerAction.innerSubscriptions.onNext(ev);

				if(outerAction.completing.get()){
				outerAction.runningComposables.decrementAndGet();
				outerAction.innerSubscriptions.onComplete();
			}
		}

		@Override
		public void onComplete() {
			//Action.log.debug("event [complete] by: " + this);
			if (checkDynamicMerge()) {
				s.cancel();

				outerAction.trySyncDispatch(null, new Consumer<Void>() {
					@Override
					public void accept(Void aVoid) {
						outerAction.runningComposables.decrementAndGet();
						outerAction.completing.set(true);
					}
				});
			}
		}

		@Override
		public String toString() {
			return "ZipAction.InnerSubscriber{lastItem=" + lastItem + ", index=" + index + ", " +
					"pending=" + pendingRequests + ", emitted=" + emittedSignals + "}";
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
			if (buffer.isComplete()) {
				scheduleTermination();
			}
		}
	}
}
