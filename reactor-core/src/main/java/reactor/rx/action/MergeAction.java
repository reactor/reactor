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
import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;

import java.util.List;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
final public class MergeAction<O> extends FanInAction<O, O, O, FanInAction.InnerSubscriber<O, O, O>> {

	public MergeAction(Dispatcher dispatcher) {
		super(dispatcher);
	}

	public MergeAction(Dispatcher dispatcher, List<? extends Publisher<? extends O>> composables) {
		super(dispatcher, composables);
	}

	@Override
	protected void doNext(O ev) {
		broadcastNext(ev);
	}

	protected InnerSubscriber<O> createSubscriber() {
		return new InnerSubscriber<O>(this);
	}


	public static final class InnerSubscriber<I> extends FanInAction.InnerSubscriber<I, I, I> {

		InnerSubscriber(FanInAction<I, I, I, ? extends FanInAction.InnerSubscriber<I, I, I>> outerAction) {
			super(outerAction);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onSubscribe(final Subscription subscription) {
			this.s = new FanInSubscription.InnerSubscription<I, I, FanInAction.InnerSubscriber<I, I, I>>(subscription, this);

			outerAction.innerSubscriptions.addSubscription(s);
			request(outerAction.innerSubscriptions.capacity().get());
		}

		@Override
		public void onNext(I ev) {
			//Action.log.debug("event [" + ev + "] by: " + this);
			outerAction.innerSubscriptions.onNext(ev);
			emittedSignals++;
			long batchSize = outerAction.runningComposables.get();
			if (batchSize > 0 && emittedSignals >= outerAction.capacity / batchSize) {
				request(emittedSignals);
			}
		}

		@Override
		public void onComplete() {
			//Action.log.debug("event [complete] by: " + this);
			s.cancel();

			Consumer<Void> completeConsumer = new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					s.toRemove = true;
					outerAction.innerSubscriptions.removeSubscription(s);
					if (outerAction.runningComposables.decrementAndGet() == 0 && !outerAction.checkDynamicMerge()) {
						outerAction.innerSubscriptions.onComplete();
					}
				}
			};

			outerAction.trySyncDispatch(null, completeConsumer);

		}

		@Override
		public String toString() {
			return "Merge.InnerSubscriber{pending=" + pendingRequests + ", emitted=" + emittedSignals + "}";
		}
	}
}
