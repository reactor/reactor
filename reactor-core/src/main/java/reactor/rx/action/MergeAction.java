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

import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.block.procedure.checked.CheckedProcedure;
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
					MultiReaderFastList.<Subscription>newList(8));
			if (processingAction != null) {
				processingAction.onSubscribe(innerSubscriptions);
			}
			for (Publisher<O> composable : composables) {
				addPublisher(composable);
			}
		} else {
			this.innerSubscriptions = new FanInSubscription<O>(upstreamAction, this);
		}

		if(upstreamAction != null){
			this.runningComposables.incrementAndGet();
		}

		onSubscribe(this.innerSubscriptions);
	}

	public void addPublisher(Publisher<O> publisher) {
		runningComposables.incrementAndGet();
		Subscriber<O> inlineMerge = new InnerSubscriber<O>(this);
		publisher.subscribe(inlineMerge);
	}

	@Override
	protected void doNext(O ev) {
		if (processingAction != null) {
			processingAction.doNext(ev);
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
			processingAction.doError(ev);
		} else {
			super.doError(ev);
		}
	}

	@Override
	protected void doComplete() {
		if (runningComposables.decrementAndGet() == 0) {
			if (processingAction == null) {
				super.doComplete();
			} else {
				processingAction.doComplete();
			}

		}
	}

	@Override
	public FanInSubscription<O> getSubscription() {
		return innerSubscriptions;
	}

	public MultiReaderFastList<Subscription> getInnerSubscriptions() {
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

		InnerSubscriber(MergeAction<O> outerAction) {
			this.outerAction = outerAction;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onSubscribe(final Subscription s) {
			outerAction.
					innerSubscriptions.
					subscriptions.
					withWriteLockAndDelegate(new CheckedProcedure<MutableList<Subscription>>() {
						@Override
						public void safeValue(MutableList<Subscription> streamSubscriptions) throws Exception {
							streamSubscriptions.add(s);
						}
					});

			outerAction.dispatch(new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					int size = outerAction.pendingRequest / outerAction.
							innerSubscriptions.
							subscriptions.size();
					int remaining =  outerAction.pendingRequest % outerAction.
							innerSubscriptions.
							subscriptions.size();
					if(size > 0){
						s.request(size+remaining);
					}
				}
			});

		}

		@Override
		public void onComplete() {
			//outerAction.innerSubscriptions.subs.remove(Thread.currentThread().getId());
			outerAction.onComplete();
		}

		@Override
		public void onError(Throwable t) {
			outerAction.onError(t);
		}

		@Override
		public void onNext(O ev) {
			outerAction.onNext(ev);
		}

		@Override
		public String toString() {
			return "InnerSubscriber";
		}

	}
}
