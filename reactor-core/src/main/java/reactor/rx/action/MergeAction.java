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
import reactor.event.dispatch.SingleThreadDispatcher;
import reactor.rx.StreamSubscription;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class MergeAction<O> extends Action<O, O> {

	final CompositeSubscription<O> innerSubscriptions;
	final AtomicInteger            runningComposables;
	final Action<O, ?>             processingAction;
	final SingleThreadDispatcher   mergedDispatcher;
	final AtomicLong               mergedCapacity;

	@SuppressWarnings("unchecked")
	public MergeAction(Dispatcher dispatcher) {
		this(dispatcher, null, null);
	}

	public MergeAction(Dispatcher dispatcher, Action<O, ?> processingAction, List<? extends Publisher<O>> composables) {
		super(dispatcher);
		this.mergedDispatcher = SingleThreadDispatcher.class.isAssignableFrom(dispatcher.getClass()) ?
				(SingleThreadDispatcher) dispatcher : null;

		int length = composables != null ? composables.size() : 0;
		this.processingAction = processingAction;

		this.innerSubscriptions = new CompositeSubscription<O>(null, this, length == 0 ?
				MultiReaderFastList.<Subscription>newList(8) :
				MultiReaderFastList.<Subscription>newList(length), null);

		this.mergedCapacity = this.innerSubscriptions.getCapacity();

		if (length > 0) {
			this.runningComposables = new AtomicInteger(processingAction == null ? length + 1 : length);
			if (processingAction != null) {
				processingAction.onSubscribe(innerSubscriptions);
			}
			for (Publisher<O> composable : composables) {
				addPublisher(composable);
			}
		} else {
			this.runningComposables = new AtomicInteger(0);
		}
	}

	public void addPublisher(Publisher<O> publisher) {
		runningComposables.incrementAndGet();
		Subscriber<O> inlineMerge = new InnerSubscriber<O>(this);
		publisher.subscribe(inlineMerge);
	}

	@Override
	protected StreamSubscription<O> createSubscription(Subscriber<O> subscriber) {
		return new CompositeSubscription<O>(this, subscriber, innerSubscriptions.subs, innerSubscriptions);
	}

	@Override
	public void onNext(O ev) {
		if (mergedDispatcher != null) {
			mergedDispatcher.dispatch(this, ev, null, null, ROUTER, this);
		} else {
			super.onNext(ev);
		}
	}

	@Override
	protected void doNext(O ev) {
		if(innerSubscriptions.getCapacity().getAndDecrement() > 0){
			innerSubscriptions.getCapacity().incrementAndGet();
		}
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
	public CompositeSubscription<O> getSubscription() {
		return innerSubscriptions;
	}

	public MultiReaderFastList<Subscription> getInnerSubscriptions() {
		return innerSubscriptions.subs;
	}

	@Override
	public String toString() {
		return super.toString() +
				"{runningComposables=" + runningComposables +
				'}';
	}

	private static class InnerSubscriber<O> implements Subscriber<O> {
		final MergeAction<O> outerAction;

		Subscription s;

		private InnerSubscriber(MergeAction<O> outerAction) {
			this.outerAction = outerAction;
		}

		@Override
		public void onSubscribe(Subscription s) {
			this.s = s;
			outerAction.innerSubscriptions.subs.add(s);
			int request = outerAction.mergedCapacity.intValue();
			if (request > 0) {
				s.request(request);
			}
		}

		@Override
		public void onComplete() {
			outerAction.innerSubscriptions.subs.remove(s);
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

	}
}
