/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package reactor.rx.action;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.rx.subscription.PushSubscription;
import reactor.tuple.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public final class ZipAction<O, V, TUPLE extends Tuple>
		extends FanInAction<O, ZipAction.Zippable<O>, V, ZipAction.InnerSubscriber<O, V>> {

	final Function<TUPLE, ? extends V> accumulator;

	int index = 0;
	int count = 0;

	Object[] toZip = new Object[2];

	AtomicBoolean completing = new AtomicBoolean();

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
		if (count == capacity || force) {
			count = 0;
			Object[] _toZip = toZip;
			toZip = new Object[toZip.length];
			broadcastNext(accumulator.apply((TUPLE) Tuple.of(_toZip)));
		}
	}


	@Override
	protected FanInSubscription<O, Zippable<O>, InnerSubscriber<O, V>> createFanInSubscription() {
		return new ZipSubscription(this,
				new ArrayList<FanInSubscription.InnerSubscription<O, Zippable<O>, ? extends InnerSubscriber<O, V>>>(8));
	}

	@Override
	protected PushSubscription<Zippable<O>> createTrackingSubscription(Subscription subscription) {
		return innerSubscriptions;
	}

	@Override
	protected void doNext(Zippable<O> ev) {
		boolean isFinishing = completing.get();
		if (toZip[ev.index] == null) {
			count++;
		}

		toZip[ev.index] = ev.data;

		if(isFinishing && count == toZip.length) {
			innerSubscriptions.onComplete();
		}else{
			broadcastTuple(false);
		}


	}

	@Override
	protected void doComplete() {
		broadcastTuple(true);
		//can receive multiple queued complete signals
		if (upstreamSubscription != null && runningComposables.get() == 0) {
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
		int newSize = innerSubscriptions.subscriptions.size() + 1;
		capacity(newSize);

		if (newSize != toZip.length) {
			Object[] previousZip = toZip;
			toZip = new Object[newSize];
			System.arraycopy(previousZip, 0, toZip, 0, newSize - 1);
		}

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
	public String toString() {
		String formatted = super.toString();
		for (int i = 0; i < toZip.length; i++) {
			if (toZip[i] != null)
				formatted += "(" + (i) + "):" + toZip[i] + ",";
		}
		return formatted.substring(0, toZip.length > 0 ? formatted.length() -1 : formatted.length());
	}

//Handling each new Publisher to zip

	public static final class InnerSubscriber<O, V> extends FanInAction.InnerSubscriber<O, Zippable<O>, V> {
		final ZipAction<O, V, ?> outerAction;
		final int                index;

		InnerSubscriber(ZipAction<O, V, ?> outerAction, int index) {
			super(outerAction);
			this.index = index;
			this.outerAction = outerAction;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onSubscribe(Subscription subscription) {
			setSubscription(new FanInSubscription.InnerSubscription<O, Zippable<O>, InnerSubscriber<O, V>>(subscription,
					this));

			outerAction.innerSubscriptions.addSubscription(s);
			if (outerAction.innerSubscriptions.capacity().get() > 0) {
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
			outerAction.innerSubscriptions.onNext(new Zippable<O>(index, ev));
		}

		@Override
		public void onComplete() {
			s.cancel();

			outerAction.trySyncDispatch(null, new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					outerAction.capacity(outerAction.runningComposables.decrementAndGet());
					if (outerAction.capacity > 0) {
						outerAction.completing.set(true);
					} else {
						outerAction.onComplete();
					}
				}
			});
		}

		@Override
		public String toString() {
			return "Zip.InnerSubscriber{index=" + index + ", " +
					"pending=" + pendingRequests + ", emitted=" + emittedSignals + "}";
		}
	}

	private final class ZipSubscription extends FanInSubscription<O, Zippable<O>, ZipAction.InnerSubscriber<O, V>> {

		public ZipSubscription(Subscriber<? super Zippable<O>> subscriber,
		                       List<InnerSubscription<O, Zippable<O>, ? extends ZipAction.InnerSubscriber<O, V>>> subs) {
			super(subscriber, subs);
		}

		@Override
		public boolean shouldRequestPendingSignals() {
			return count == 0;
		}

		@Override
		public void doPendingRequest() {
			if (maxCapacity > 0) {
				request(maxCapacity);
			}
		}

		@Override
		protected void onRequest(long integer) {
			try {
				broadcastTuple(false);

				super.onRequest(integer);
			} catch (Throwable e) {
				doError(e);
			}
		}

		@Override
		protected void parallelRequest(long elements) {
			super.parallelRequest(1);
			if (buffer.isComplete()) {
				scheduleTermination();
			}
		}
	}

	public static final class Zippable<O> {
		final int index;
		final O   data;

		public Zippable(int index, O data) {
			this.index = index;
			this.data = data;
		}
	}

}
