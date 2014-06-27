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

import com.gs.collections.api.block.predicate.Predicate;
import com.gs.collections.impl.block.procedure.checked.CheckedProcedure;
import com.gs.collections.impl.list.mutable.MultiReaderFastList;
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class FanOutSubscription<O> extends StreamSubscription<O> {
	private final MultiReaderFastList<StreamSubscription<O>> subscriptions = MultiReaderFastList.newList(2);

	public FanOutSubscription(Stream<O> publisher, StreamSubscription<O> streamSubscriptionA,
	                          StreamSubscription<O> streamSubscriptionB) {
		super(publisher, null, null);
		subscriptions.add(streamSubscriptionA);
		subscriptions.add(streamSubscriptionB);
	}

	@Override
	public void onComplete() {
		subscriptions.forEach(new CheckedProcedure<StreamSubscription<O>>() {
			@Override
			public void safeValue(StreamSubscription<O> subscription) throws Exception {
				try {
					subscription.onComplete();
				} catch (Throwable throwable) {
					subscription.onError(throwable);
				}
			}
		});
	}

	@Override
	public void onNext(O ev) {
		subscriptions.forEach(new CheckedProcedure<StreamSubscription<O>>() {
			@Override
			public void safeValue(StreamSubscription<O> subscription) throws Exception {
				try {
					if (subscription.isComplete()) {
						return;
					}

					subscription.onNext(ev);

				} catch (Throwable throwable) {
					subscription.onError(throwable);
				}
			}
		});
	}

	@Override
	public void onError(final Throwable ev) {
		subscriptions.forEach(new CheckedProcedure<StreamSubscription<O>>() {
			@Override
			public void safeValue(StreamSubscription<O> subscription) throws Exception {
				subscription.onError(ev);
			}
		});
	}

	public MultiReaderFastList<StreamSubscription<O>> getSubscriptions() {
		return subscriptions;
	}

	@Override
	public boolean isComplete() {
		return subscriptions.select(new Predicate<StreamSubscription<O>>() {
			@Override
			public boolean accept(StreamSubscription<O> subscription)  {
				return !subscription.isComplete();
			}
		}).isEmpty();
	}
}
