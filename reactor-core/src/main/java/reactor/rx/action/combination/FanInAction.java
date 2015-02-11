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
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.dispatch.TailRecurseDispatcher;
import reactor.fn.Consumer;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.action.support.NonBlocking;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * The best moment of my life so far, not.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
abstract public class FanInAction<I, E, O, SUBSCRIBER extends FanInAction.InnerSubscriber<I, E, O>> extends Action<E,
		O> {


	final FanInSubscription<I, E, O, SUBSCRIBER>     innerSubscriptions;
	final Iterable<? extends Publisher<? extends I>> composables;

	final static protected int NOT_STARTED = 0;
	final static protected int RUNNING     = 1;
	final static protected int COMPLETING  = 2;

	final AtomicInteger status = new AtomicInteger();
	final protected Dispatcher dispatcher;


	DynamicMergeAction<?, ?> dynamicMergeAction = null;

	@SuppressWarnings("unchecked")
	public FanInAction(Dispatcher dispatcher) {
		this(dispatcher, null);
	}

	public FanInAction(Dispatcher dispatcher,
	                   Iterable<? extends Publisher<? extends I>> composables) {
		super();
		this.dispatcher = SynchronousDispatcher.INSTANCE == dispatcher ?
				Environment.tailRecurse() : dispatcher;
		this.composables = composables;
		this.upstreamSubscription = this.innerSubscriptions = createFanInSubscription();
	}

	@Override
	public void subscribe(Subscriber<? super O> subscriber) {
		doSubscribe(this.innerSubscriptions);
		super.subscribe(subscriber);
	}

	public void addPublisher(Publisher<? extends I> publisher) {
		InnerSubscriber<I, E, O> inlineMerge = createSubscriber();
		inlineMerge.pendingRequests =  innerSubscriptions.pendingRequestSignals() / (innerSubscriptions.runningComposables + 1);
		publisher.subscribe(inlineMerge);
	}


	@Override
	protected void doStart(long pending) {
		if (dynamicMergeAction != null) {
			dispatcher.dispatch(pending, dynamicMergeAction.getSubscription(), null);
		}
	}

	public void scheduleCompletion() {
		if (status.compareAndSet(NOT_STARTED, COMPLETING)) {
			innerSubscriptions.serialComplete();
		} else {
			status.set(COMPLETING);
			if (innerSubscriptions.runningComposables == 0) {
				innerSubscriptions.serialComplete();
			}
		}
	}

	@Override
	public void cancel() {
		if (dynamicMergeAction != null) {
			dynamicMergeAction.cancel();
		}
		innerSubscriptions.cancel();
	}

	public Action<?, ?> dynamicMergeAction() {
		return dynamicMergeAction;
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		if (status.compareAndSet(NOT_STARTED, RUNNING)) {
			innerSubscriptions.maxCapacity(capacity);
			if (composables != null) {
				if (innerSubscriptions.runningComposables > 0) {
					innerSubscriptions.cancel();
					return;
				}
				capacity(initUpstreamPublisherAndCapacity());
			}
		}
	}

	protected long initUpstreamPublisherAndCapacity() {
		long maxCapacity = capacity;
		for (Publisher<? extends I> composable : composables) {
			if (Stream.class.isAssignableFrom(composable.getClass())) {
				maxCapacity = Math.min(maxCapacity, ((Stream<?>) composable).getCapacity());
			}
			addPublisher(composable);
		}
		return maxCapacity;
	}


	protected final boolean checkDynamicMerge() {
		return dynamicMergeAction != null && dynamicMergeAction.isPublishing();
	}

	@Override
	public void onNext(E ev) {
		super.onNext(ev);
		if(innerSubscriptions.shouldRequestPendingSignals()){
			long left = upstreamSubscription.pendingRequestSignals();
			if (left > 0l) {
				upstreamSubscription.updatePendingRequests(-left);
				dispatcher.dispatch(left, upstreamSubscription, null);
			}
		}
	}

	@Override
	public void requestMore(long n) {
		checkRequest(n);
		dispatcher.dispatch(n, upstreamSubscription, null);
	}

	@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		//	innerSubscriptions.request(elements);
		elements = Math.max(this.capacity, elements);
		super.requestUpstream(capacity, terminated, elements);
		if (dynamicMergeAction != null) {
			dynamicMergeAction.requestUpstream(capacity, terminated, elements);
		}
	}

	@Override
	public final Dispatcher getDispatcher() {
		return TailRecurseDispatcher.class == dispatcher.getClass() ? SynchronousDispatcher.INSTANCE : dispatcher;
	}

	@Override
	public String toString() {
		return super.toString() +
				"{runningComposables=" + innerSubscriptions.runningComposables + "}";
	}

	protected FanInSubscription<I, E, O, SUBSCRIBER> createFanInSubscription() {
		return new FanInSubscription<I, E, O, SUBSCRIBER>(this);
	}

	@Override
	public FanInSubscription<I, E, O, SUBSCRIBER> getSubscription() {
		return innerSubscriptions;
	}

	protected abstract InnerSubscriber<I, E, O> createSubscriber();

	public abstract static class InnerSubscriber<I, E, O> implements Subscriber<I>, NonBlocking, Consumer<Long> {
		final FanInAction<I, E, O, ? extends InnerSubscriber<I, E, O>> outerAction;

		int sequenceId;

		FanInSubscription.InnerSubscription<I, E, InnerSubscriber<I, E, O>> s;

		long pendingRequests = 0;
		long emittedSignals  = 0;
		volatile int terminated = 0;

		final static AtomicIntegerFieldUpdater<InnerSubscriber> TERMINATE_UPDATER =
				AtomicIntegerFieldUpdater.newUpdater(InnerSubscriber.class, "terminated");

		InnerSubscriber(FanInAction<I, E, O, ? extends InnerSubscriber<I, E, O>> outerAction) {
			this.outerAction = outerAction;
		}

		public void cancel() {
			Subscription s = this.s;
			if(s != null){
				s.cancel();
			}
		}

		@SuppressWarnings("unchecked")
		void setSubscription(FanInSubscription.InnerSubscription s) {
			this.s = s;
			this.sequenceId = outerAction.innerSubscriptions.addSubscription(this);
		}

		public void accept(Long pendingRequests) {
			try {
				if (pendingRequests > 0) {
					request(pendingRequests);
				}
			} catch (Throwable e) {
				outerAction.onError(e);
			}
		}


		public void request(long n) {
			if (s == null || n <= 0) return;
			if ((pendingRequests += n) < 0l) {
				pendingRequests = Long.MAX_VALUE;
			}
			emittedSignals = 0;
			s.request(n);
		}


		@Override
		public void onError(Throwable t) {
			FanInSubscription.RUNNING_COMPOSABLE_UPDATER.decrementAndGet(outerAction.innerSubscriptions);
			outerAction.innerSubscriptions.serialError(t);
		}

		@Override
		public void onComplete() {
			//Action.log.debug("event [complete] by: " + this);
			if (TERMINATE_UPDATER.compareAndSet(this, 0, 1)) {
				s.cancel();
				outerAction.status.set(COMPLETING);
				long left = FanInSubscription.RUNNING_COMPOSABLE_UPDATER.decrementAndGet(outerAction.innerSubscriptions);
				left = left < 0l ? 0l : left;

				outerAction.innerSubscriptions.remove(sequenceId);
				if (left == 0) {
					if (!outerAction.checkDynamicMerge()){
						outerAction.innerSubscriptions.serialComplete();
					}
				}
			}

		}

		@Override
		public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
			return outerAction.isReactivePull(dispatcher, producerCapacity);
		}

		@Override
		public long getCapacity() {
			return outerAction.capacity;
		}

		@Override
		public String toString() {
			return "FanInAction.InnerSubscriber{pending=" + pendingRequests + ", emitted=" + emittedSignals + "}";
		}
	}

}
