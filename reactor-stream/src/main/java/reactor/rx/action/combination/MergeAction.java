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
package reactor.rx.action.combination;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;

import java.util.List;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
final public class MergeAction<O> extends FanInAction<O, O, O, MergeAction.InnerSubscriber<O>> {

	public MergeAction(Dispatcher dispatcher) {
		super(dispatcher);
	}

	public MergeAction(Dispatcher dispatcher, List<? extends Publisher<? extends O>> publishers) {
		super(dispatcher, publishers);
	}

	@Override
	protected void doNext(O ev) {
		broadcastNext(ev);
	}

	protected InnerSubscriber<O> createSubscriber() {
		return new InnerSubscriber<O>(this);
	}


	public static final class InnerSubscriber<I> extends FanInAction.InnerSubscriber<I, I, I> {

		InnerSubscriber(FanInAction<I, I, I, InnerSubscriber<I>> outerAction) {
			super(outerAction);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onSubscribe(final Subscription subscription) {
			setSubscription(
					new FanInSubscription.InnerSubscription<I, I, FanInAction.InnerSubscriber<I,I,I>>(subscription, this)
			);
			if (outerAction.dynamicMergeAction != null) {
				outerAction.dynamicMergeAction.decrementWip();
			}
			long toRequest = pendingRequests;
			if (toRequest > 0) {
				pendingRequests = 0;
				request(toRequest);
			}
		}

		@Override
		public void onNext(I ev) {
			//LoggerFactory.getLogger(getClass()).debug("event [" + ev + "] by: " + this);
			outerAction.innerSubscriptions.serialNext(ev);
			emittedSignals++;

			if(--pendingRequests < 0){
				pendingRequests = 0l;
			}
		}



		@Override
		public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
			return false;
		}


		@Override
		public String toString() {
			return "Merge.InnerSubscriber{pending=" + pendingRequests + ", emitted=" + emittedSignals + "}";
		}
	}
}
