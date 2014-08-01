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

import com.gs.collections.impl.list.mutable.MultiReaderFastList;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class MergeAction<O> extends Action<O, O> {

	final FanInSubscription<O> innerSubscriptions;
	final AtomicInteger        runningComposables;
	final Action<O, ?>         processingAction;

	@SuppressWarnings("unchecked")
	public MergeAction(Dispatcher dispatcher, Action<?, O> upstreamAction) {
		this(dispatcher, upstreamAction, null, null);
	}

	public MergeAction(Dispatcher dispatcher, Action<?, O> upstreamAction,
	                   Action<O, ?> processingAction, List<? extends Publisher<O>> composables) {
		super(dispatcher);

		int length = composables != null ? composables.size() : 0;
		this.processingAction = processingAction;
		this.runningComposables = new AtomicInteger(0);

		if (length > 0) {
			this.innerSubscriptions = new FanInSubscription<O>(upstreamAction, this,
					MultiReaderFastList.<FanInSubscription.InnerSubscription>newList(8));

			onSubscribe(this.innerSubscriptions);

			if (processingAction != null) {
				processingAction.onSubscribe(innerSubscriptions);
			}

			for (Publisher<O> composable : composables) {
				addPublisher(composable);
			}
		} else {
			this.innerSubscriptions = new FanInSubscription<O>(upstreamAction, this);

			onSubscribe(this.innerSubscriptions);
		}

		if (upstreamAction != null) {
			this.runningComposables.incrementAndGet();
		}


	}

	public void addPublisher(Publisher<O> publisher) {
		runningComposables.incrementAndGet();
		Subscriber<O> inlineMerge = new InnerSubscriber<O>(this);
		publisher.subscribe(inlineMerge);
	}

	@Override
	protected void doNext(O ev) {
		if (processingAction != null) {
			processingAction.onNext(ev);
		} else {
			broadcastNext(ev);
		}
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		if (processingAction != null) {
			processingAction.doSubscribe(subscription);
		} else {
			super.doSubscribe(subscription);
		}
	}

	@Override
	protected void doError(Throwable ev) {
		if (processingAction != null) {
			processingAction.onError(ev);
		} else {
			super.doError(ev);
		}
	}

	@Override
	protected void doComplete() {
		if (processingAction == null) {
			super.doComplete();
		} else {
			processingAction.onComplete();
		}
	}

	@Override
	public FanInSubscription<O> getSubscription() {
		return innerSubscriptions;
	}

	public MultiReaderFastList<FanInSubscription.InnerSubscription> getInnerSubscriptions() {
		return innerSubscriptions.subscriptions;
	}

	@Override
	public String toString() {
		return super.toString() +
				"{runningComposables=" + runningComposables +
				'}';
	}

	private static class InnerSubscriber<O> implements Subscriber<O> {
		final MergeAction<O> outerAction;
		FanInSubscription.InnerSubscription s;

		InnerSubscriber(MergeAction<O> outerAction) {
			this.outerAction = outerAction;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onSubscribe(final Subscription subscription) {
			this.s = new FanInSubscription.InnerSubscription(subscription);

			outerAction.innerSubscriptions.addSubscription(s);
			outerAction.dispatch(new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					long currentCapacity = outerAction.innerSubscriptions.getCapacity().get();
					if (currentCapacity > 0) {
						int size = outerAction.innerSubscriptions.subscriptions.size();
						if (size == 0) return;

						int batchSize = outerAction.batchSize / size;
						int toRequest = outerAction.batchSize % size + batchSize;

						s.request(toRequest > currentCapacity ? toRequest : (int)currentCapacity);
					}
				}
			});


		}

		@Override
		public void onComplete() {
			s.toRemove = true;
			if (outerAction.runningComposables.decrementAndGet() == 0) {
				outerAction.innerSubscriptions.onComplete();
			}
		}

		@Override
		public void onError(Throwable t) {
			outerAction.runningComposables.decrementAndGet();
			outerAction.innerSubscriptions.onError(t);
		}

		@Override
		public void onNext(O ev) {
			outerAction.innerSubscriptions.onNext(ev);
		}

		@Override
		public String toString() {
			return "InnerSubscriber";
		}
	}

}
