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

import com.gs.collections.impl.block.procedure.checked.CheckedProcedure;
import com.gs.collections.impl.list.mutable.MultiReaderFastList;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.function.Function;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class ZipAction<O, V> extends FanInAction<O, V> {

	final Function<List<O>, V> accumulator;
	final List<O>              buffer;

	final Consumer<Integer> upstreamConsumer = new Consumer<Integer>() {
		@Override
		public void accept(Integer integer) {
			try {
				if (!buffer.isEmpty()) {
					broadcastNext(accumulator.apply(buffer));
					buffer.clear();
				}
				requestConsumer.accept(integer);
			} catch (Throwable e) {
				doError(e);
			}
		}
	};

	public ZipAction(Dispatcher dispatcher,
	                 Function<List<O>, V> accumulator, List<? extends Publisher<O>> composables) {
		super(dispatcher, composables);
		this.buffer = new ArrayList<O>(composables != null ? composables.size() : 8);
		this.accumulator = accumulator;
	}

	@Override
	protected FanInSubscription<O> createFanInSubscription() {
		return new ZipSubscription<O>(this, MultiReaderFastList.<FanInSubscription.InnerSubscription>newList(8));
	}

	@Override
	protected void doNext(O ev) {
		buffer.add(ev);
		if (currentNextSignals == batchSize) {
			broadcastNext(accumulator.apply(new ArrayList<O>(buffer)));
			buffer.clear();
		}
	}

	@Override
	protected FanInAction.InnerSubscriber<O, V> createSubscriber() {
		batchSize = innerSubscriptions.subscriptions.size() + 1;
		return new ZipAction.InnerSubscriber<O, V>(this);
	}


	@Override
	protected void onRequest(int n) {
		dispatch(n, upstreamConsumer);
	}


	@Override
	public String toString() {
		return super.toString() +
				"{staged=" + buffer.size() +
				'}';
	}

	private static class InnerSubscriber<O, V> extends FanInAction.InnerSubscriber<O, V> {
		InnerSubscriber(ZipAction<O, V> outerAction) {
			super(outerAction);
		}

		@Override
		public void onSubscribe(Subscription subscription) {
			this.s = new FanInSubscription.InnerSubscription(subscription);

			outerAction.innerSubscriptions.addSubscription(s);
			outerAction.dispatch(new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					if (outerAction.innerSubscriptions.getCapacity().get() > 0) {
						s.request(1);
					}
				}
			});
		}

		@Override
		public void onComplete() {
			outerAction.capacity(outerAction.getMaxCapacity() - 1);
			super.onComplete();
		}
	}

	private static class ZipSubscription<O> extends FanInSubscription<O> {
		public ZipSubscription(Subscriber<O> subscriber, MultiReaderFastList<InnerSubscription> subs) {
			super(subscriber, subs);
		}

		@Override
		protected void parallelRequest(final int elements) {
			final int parallel = subscriptions.size();
			if (parallel > 0) {
				subscriptions.forEach(new CheckedProcedure<Subscription>() {
					@Override
					public void safeValue(Subscription subscription) throws Exception {
						subscription.request(1);
					}
				});

				pruneObsoleteSubscriptions();
			}
		}
	}
}
