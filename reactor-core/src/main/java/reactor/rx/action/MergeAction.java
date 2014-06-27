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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SingleThreadDispatcher;
import reactor.function.Consumer;
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class MergeAction<O> extends Action<O, O> {

	final FanInSubscription<O>   innerSubscriptions;
	final AtomicInteger          runningComposables;
	final Action<O, ?>           processingAction;
	final SingleThreadDispatcher mergedDispatcher;

	@SuppressWarnings("unchecked")
	public MergeAction(Dispatcher dispatcher, Action<?, O> upstreamAction) {
		this(dispatcher, upstreamAction, null, null);
	}

	public MergeAction(Dispatcher dispatcher, Action<?, O> upstreamAction,
	                   Action<O, ?> processingAction, List<? extends Publisher<O>> composables) {
		super(dispatcher);
		this.mergedDispatcher = SingleThreadDispatcher.class.isAssignableFrom(dispatcher.getClass()) ?
				(SingleThreadDispatcher) dispatcher : null;

		int length = composables != null ? composables.size() : 0;
		this.processingAction = processingAction;


		if (length > 0) {
			this.innerSubscriptions = new FanInSubscription<O>(upstreamAction, this, new HashMap<Long,
					Subscription>(length));
			this.runningComposables = new AtomicInteger(processingAction == null ? length + 1 : length);
			if (processingAction != null) {
				processingAction.onSubscribe(innerSubscriptions);
			}
			for (Publisher<O> composable : composables) {
				addPublisher(composable);
			}
		} else {
			this.innerSubscriptions = new FanInSubscription<O>(upstreamAction, this);
			this.runningComposables = new AtomicInteger(0);
		}
		onSubscribe(this.innerSubscriptions);
	}


	protected void requestUpstream() {
		long pendingSubAvail = innerSubscriptions.pendingSubscriptionAvailable.get();
		if (pendingSubAvail <= 0) return;

		if (Integer.MAX_VALUE < pendingSubAvail) {
			long batches = pendingSubAvail / Integer.MAX_VALUE;
			for (long i = 0; i < batches; i++) {
				innerSubscriptions.request(Integer.MAX_VALUE);
			}
			int remaining = (int) (pendingSubAvail % Integer.MAX_VALUE);
			if (remaining > 0) {
				innerSubscriptions.request(remaining);
			}
		} else {
			innerSubscriptions.request((int) pendingSubAvail);
		}
	}

	@Override
	protected void requestUpstream(AtomicLong capacity, boolean terminated, int elements) {
		if(innerSubscriptions.subs.isEmpty()){
			innerSubscriptions.pendingSubscriptionAvailable.addAndGet(elements);
		}
		super.requestUpstream(capacity,terminated,elements);
	}

	public void addPublisher(Publisher<O> publisher) {
		runningComposables.incrementAndGet();
		Subscriber<O> inlineMerge = new InnerSubscriber<O>(this);
		publisher.subscribe(inlineMerge);
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

	public Collection<Subscription> getInnerSubscriptions() {
		return Collections.unmodifiableCollection(innerSubscriptions.subs.values());
	}

	@Override
	public String toString() {
		return super.toString() +
				"{runningComposables=" + runningComposables +
				'}';
	}

	private static class InnerSubscriber<O> implements Subscriber<O> {
		final MergeAction<O> outerAction;
		final AtomicLong     pendingSubscriptions;

		long         resourceId;
		Subscription s;
		int          batchSize;

		private InnerSubscriber(MergeAction<O> outerAction) {
			this.outerAction = outerAction;
			this.pendingSubscriptions = outerAction.innerSubscriptions.pendingSubscriptionAvailable;
			this.batchSize = outerAction.batchSize;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void onSubscribe(Subscription s) {
			this.s = s;
			final FanInSubscription<O> fanInSubscription = outerAction.innerSubscriptions;
			if (StreamSubscription.class.isAssignableFrom(s.getClass())
					&& Action.class.isAssignableFrom(((StreamSubscription<O>) s).getPublisher().getClass())) {

				final StreamSubscription<O> streamSub = (StreamSubscription<O>) s;
				resourceId = ((Action<?, O>) ((StreamSubscription<O>) s).getPublisher()).resourceID;
				s = new InnerSubscription<>(this, streamSub, fanInSubscription, s);

			} else {
				resourceId = fanInSubscription.subs.size();
			}
			fanInSubscription.subs.put(resourceId, s);
			outerAction.requestUpstream();
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
			return "InnerSubscriber{" +
					"resourceId=" + resourceId +
					'}';
		}

	}

	private static class InnerSubscription<O> extends StreamSubscription<O> {
		private final InnerSubscriber       innerSubscriber;
		private final StreamSubscription<O> streamSub;
		private final FanInSubscription<O>  fanInSubscription;
		private final Subscription          upstreamSubscription;

		public InnerSubscription(InnerSubscriber innerSubscriber, StreamSubscription<O> streamSub,
		                         FanInSubscription<O> fanInSubscription, Subscription upstreamSubscription) {
			super(null, null);
			this.innerSubscriber = innerSubscriber;
			this.streamSub = streamSub;
			this.fanInSubscription = fanInSubscription;
			this.upstreamSubscription = upstreamSubscription;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void request(int elements) {
			streamSub.getPublisher().dispatch(elements, new Consumer<Integer>() {
				@Override
				public void accept(Integer i) {
					Subscription sub = fanInSubscription.subs.get(Thread.currentThread().getId());
					if (sub != null) {

						if (InnerSubscription.class.isAssignableFrom(sub.getClass())) {
							((InnerSubscription<O>) sub).upstreamSubscription.request(i);
						} else {
							sub.request(i);
						}

					} else {
						innerSubscriber.pendingSubscriptions.addAndGet(i);
					}
				}
			});
		}

		@Override
		public void cancel() {
			streamSub.cancel();
		}

		@Override
		public Stream<?> getPublisher() {
			return streamSub.getPublisher();
		}

		@Override
		public Subscriber<O> getSubscriber() {
			return streamSub.getSubscriber();
		}
	}
}
