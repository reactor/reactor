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
package reactor.rx.action.combination;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.fn.Function;
import reactor.fn.tuple.Tuple;
import reactor.rx.subscription.PushSubscription;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public final class CombineLatestAction<O, V, TUPLE extends Tuple>
		extends FanInAction<O, CombineLatestAction.Zippable<O>, V, CombineLatestAction.InnerSubscriber<O, V>> {

	private static final Object EMPTY_ZIPPED_DATA = new Object();

	final Function<TUPLE, ? extends V> accumulator;

	int index = 0;

	Object[] toZip = new Object[2];

	public CombineLatestAction(Dispatcher dispatcher,
	                           Function<TUPLE, ? extends V> accumulator, Iterable<? extends Publisher<? extends O>>
			composables) {
		super(dispatcher, composables);
		this.accumulator = accumulator;
	}

	@SuppressWarnings("unchecked")
	protected void broadcastTuple() {
		if (!checkAllFilled()) return;

		Object[] _toZip = toZip;

		V res = accumulator.apply((TUPLE) Tuple.of(_toZip));

		if (res != null) {
			broadcastNext(res);


		}
	}

	private boolean checkAllFilled() {
		for (int i = 0; i < toZip.length; i++) {
			if (toZip[i] == null) {
				return false;
			}
		}
		return true;
	}


	@Override
	protected FanInSubscription<O, Zippable<O>, V, InnerSubscriber<O, V>> createFanInSubscription() {
		return new FanInSubscription<>(this);
	}

	@Override
	protected PushSubscription<Zippable<O>> createTrackingSubscription(Subscription subscription) {
		return innerSubscriptions;
	}

	@Override
	protected void doNext(Zippable<O> ev) {
		toZip[ev.index] = ev.data == null ? EMPTY_ZIPPED_DATA : ev.data;

		broadcastTuple();
	}

	@Override
	public void scheduleCompletion() {
		//let the zip logic complete
	}

	@Override
	protected void doComplete() {
			//can receive multiple queued complete signals
			cancel();
			broadcastComplete();
	}

	@Override
	protected InnerSubscriber<O, V> createSubscriber() {
		int newSize = innerSubscriptions.subscriptions.size() + 1;
		capacity(newSize);

		if (newSize > toZip.length) {
			Object[] previousZip = toZip;
			toZip = new Object[newSize];
			System.arraycopy(previousZip, 0, toZip, 0, newSize - 1);
		}

		return new CombineLatestAction.InnerSubscriber<>(this, index++);
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
		return formatted.substring(0, formatted.length());
	}

//Handling each new Publisher to zip

	public static final class InnerSubscriber<O, V> extends FanInAction.InnerSubscriber<O, Zippable<O>, V> {
		final CombineLatestAction<O, V, ?> outerAction;
		final int                          index;

		InnerSubscriber(CombineLatestAction<O, V, ?> outerAction, int index) {
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
			if (pendingRequests > 0) {
				request(pendingRequests);
			}
			if (outerAction.dynamicMergeAction != null) {
				outerAction.dynamicMergeAction.decrementWip();
			}
		}

		@Override
		public void onNext(O ev) {
			if (--pendingRequests > 0) pendingRequests = 0;
			//emittedSignals++;
			outerAction.innerSubscriptions.serialNext(new Zippable<O>(index, ev));
		}

		@Override
		public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
			return false;
		}

		@Override
		public String toString() {
			return "CombineLatest.InnerSubscriber{index=" + index + ", " +
					"pending=" + pendingRequests + ", emitted=" + emittedSignals + "}";
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
