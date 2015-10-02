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
import reactor.core.support.Bounded;

import java.util.List;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
final public class MergeAction<O> extends FanInAction<O, O, O, MergeAction.InnerSubscriber<O>> {

	public MergeAction() {
		super();
	}

	public MergeAction(List<? extends Publisher<? extends O>> publishers) {
		super(publishers);
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
			  new FanInSubscription.InnerSubscription<I, I, FanInAction.InnerSubscriber<I, I, I>>(subscription, this)
			);
			if (outerAction.dynamicMergeAction != null) {
				outerAction.dynamicMergeAction.decrementWip();
			}

			final long toRequest;

			if(outerAction.publishers == null){
				toRequest = pendingRequests;
			}else{
				toRequest = getCapacity() != Long.MAX_VALUE ?
				  Math.max(1, getCapacity() / outerAction.innerSubscriptions.runningComposables) :
				  Long.MAX_VALUE;
			}

			pendingRequests = 0;
			request(toRequest);
		}

		@Override
		public void onNext(I ev) {
			//LoggerFactory.getLogger(getClass()).debug("event [" + ev + "] by: " + this);
			outerAction.innerSubscriptions.serialNext(ev);
			emittedSignals++;

			if (Long.MAX_VALUE != pendingRequests && --pendingRequests < 0) {
				pendingRequests = 0;
				long toRequest = outerAction.innerSubscriptions.pendingRequestSignals();
					request((toRequest > 0 ? toRequest : getCapacity())
					  / Math.max(outerAction.innerSubscriptions.runningComposables, 1));
			}
		}


		@Override
		public boolean isExposedToOverflow(Bounded upstream) {
			return false;
		}


		@Override
		public String toString() {
			return "Merge.InnerSubscriber{pending=" + pendingRequests + ", emitted=" + emittedSignals + "}";
		}
	}
}
