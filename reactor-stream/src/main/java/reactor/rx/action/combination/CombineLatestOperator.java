/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.rx.action.combination;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.processor.rb.disruptor.RingBuffer;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.core.support.SignalType;
import reactor.core.support.internal.PlatformDependent;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.fn.tuple.Tuple;

/**
 * A zip operator to combine 1 by 1 each upstream in parallel given the combinator function.
 * @author Stephane Maldini
 * @since 2.1
 */
public final class CombineLatestOperator<TUPLE extends Tuple, V>
		implements Function<Subscriber<? super V>, Subscriber<? super Publisher[]>> {

	final Function<? super TUPLE, ? extends V> combinator;
	final int                                  bufferSize;

	public CombineLatestOperator(final Function<? super TUPLE, ? extends V> combinator, int bufferSize) {
		this.combinator = combinator;
		this.bufferSize = bufferSize;
	}

	@Override
	public Subscriber<? super Publisher[]> apply(Subscriber<? super V> t) {
		return new CombineLatestAction<>(t, combinator, bufferSize);
	}

	static final class CombineLatestAction<TUPLE extends Tuple, V> extends SubscriberWithDemand<Publisher[], V> {

		final Function<? super TUPLE, ? extends V> combinator;
		final int                                  bufferSize;
		final int                                  limit;

		@SuppressWarnings("unused")
		private volatile Throwable error;

		private volatile boolean cancelled;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<CombineLatestAction, Throwable> ERROR =
				PlatformDependent.newAtomicReferenceFieldUpdater(CombineLatestAction.class, "error");

		private InnerSubscriber<?>[] subscribers;

		@SuppressWarnings("unused")
		private volatile int running;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<CombineLatestAction> RUNNING =
				AtomicIntegerFieldUpdater.newUpdater(CombineLatestAction.class, "running");

		@SuppressWarnings("unused")
		private volatile int started;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<CombineLatestAction> STARTED =
				AtomicIntegerFieldUpdater.newUpdater(CombineLatestAction.class, "started");

		@SuppressWarnings("unused")
		volatile int completed;
		final static AtomicIntegerFieldUpdater<CombineLatestAction> WIP_COMPLETED =
				AtomicIntegerFieldUpdater.newUpdater(CombineLatestAction.class, "completed");

		Object[] valueCache;
		boolean completeTuple = false;

		public CombineLatestAction(Subscriber<? super V> actual,
				Function<? super TUPLE, ? extends V> combinator,
				int bufferSize) {
			super(actual);
			this.combinator = combinator;
			this.bufferSize = bufferSize;
			this.limit = Math.max(1, bufferSize / 2);
		}

		@Override
		protected void doOnSubscribe(Subscription s) {
			subscriber.onSubscribe(this);
			requestMore(1L);
		}

		@Override
		@SuppressWarnings("unchecked")
		protected void doNext(Publisher[] sources) {
			if (TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_SUCCESS)) {
				checkedCancel();

				int n = sources.length;
				if (n == 0) {
					subscriber.onComplete();
					return;
				}

				subscribers = new InnerSubscriber[n];
				completed = n;
				valueCache = new Object[n];

				int i;
				InnerSubscriber<?> inner;
				for (i = 0; i < n; i++) {
					inner = new InnerSubscriber<>(sources[i], this);

					if (sources[i] instanceof Supplier) {
						inner.latestValue = ((Supplier<?>) sources[i]).get();
						inner.done = true;
						WIP_COMPLETED.decrementAndGet(this);
					}

					subscribers[i] = inner;
				}

				if (STARTED.get(this) == 1) {
					for (i = 0; i < n; i++) {
						if (!(sources[i] instanceof Supplier)) {
							sources[i].subscribe(subscribers[i]);
						}
					}
				}
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		protected void doRequest(long n) {
			BackpressureUtils.getAndAdd(REQUESTED, this, n);
			InnerSubscriber[] subscribers = this.subscribers;
			if (STARTED.compareAndSet(this, 0, 1)) {
				if (subscribers != null) {
					for (int i = 0; i < subscribers.length; i++) {
						if (!(subscribers[i].upstream instanceof Supplier)) {
							subscribers[i].upstream.subscribe(subscribers[i]);
						}
					}
				}
				drain();
			}
		}

		@Override
		protected void doCancel() {
			if (!cancelled) {
				cancelled = true;
				if (RUNNING.getAndIncrement(this) == 0) {
					subscription.cancel();
					cancelStates();
				}
			}
		}

		@Override
		protected void checkedComplete() {
			InnerSubscriber[] s = subscribers;
			if (s == null || s.length == 0) {
				if (RUNNING.getAndIncrement(this) == 0) {
					subscriber.onComplete();
				}
			}
		}

		@Override
		protected void checkedError(Throwable throwable) {
			doOnSubscriberError(throwable);
		}

		@Override
		protected void doOnSubscriberError(Throwable throwable) {
			if (!ERROR.compareAndSet(this, null, throwable)) {
				throw ReactorFatalException.create(throwable);
			}
			subscriber.onError(throwable);
		}

		void drain() {
			InnerSubscriber[] subscribers = this.subscribers;
			if (subscribers == null) {
				return;
			}
			if (RUNNING.getAndIncrement(this) == 0) {
				drainLoop(subscribers);
			}
		}

		@SuppressWarnings("unchecked")
		void drainLoop(InnerSubscriber[] inner) {

			final Subscriber<? super V> actual = this.subscriber;
			int missed = 1;
			for (; ; ) {
				if (checkImmediateTerminate()) {
					return;
				}

				int n = inner.length;
				int replenishMain = 0;
				long r = getRequested();

				InnerSubscriber<?> state;

				for (; ; ) {

					final Object[] tuple = valueCache;
					boolean completeTuple = true;
					boolean updated = false;
					int i;
					for (i = 0; i < n; i++) {
						state = inner[i];

						Object next = state.readNext();

						if (next == null) {
							if (state.isTerminated() && WIP_COMPLETED.get(this) == 0) {
								actual.onComplete();
								return;
							}

							completeTuple = false;
							continue;
						}

						tuple[i] = next;
						updated = true;
					}

					if (completeTuple) {
						this.completeTuple = true;
					}
					else if (this.completeTuple) {
						completeTuple = updated;
					}

					if (r != 0 && completeTuple) {
						try {
							actual.onNext(combinator.apply((TUPLE) Tuple.of(tuple)));
							if (r != Long.MAX_VALUE) {
								r--;
							}
							replenishMain++;
						}
						catch (Throwable e) {
							actual.onError(Exceptions.addValueAsLastCause(e, tuple));
							return;
						}

						// consume 1 from each and check if complete

						for (i = 0; i < n; i++) {

							if (checkImmediateTerminate()) {
								return;
							}

							state = inner[i];
							state.requestMore();

							if (state.readNext() == null && state.isTerminated() && WIP_COMPLETED.get(this) == 0) {
								actual.onComplete();
								return;
							}
						}
					}
					else {
						break;
					}
				}

				if (replenishMain > 0) {
					BackpressureUtils.getAndSub(REQUESTED, this, replenishMain);
				}

				missed = RUNNING.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		void cancelStates() {
			InnerSubscriber[] inner = subscribers;
			for (int i = 0; i < inner.length; i++) {
				inner[i].cancel();
			}
		}

		boolean checkImmediateTerminate() {
			if (cancelled) {
				subscription.cancel();
				cancelStates();
				return true;
			}
			Throwable e = error;
			if (e != null) {
				try {
					subscriber.onError(error);
				}
				finally {
					cancelStates();
				}
				return true;
			}
			return false;
		}

	}

	static final class InnerSubscriber<V> extends BaseSubscriber<Object> implements ReactiveState.Bounded,
	                                                                                ReactiveState.Upstream {

		final CombineLatestAction<?, V> parent;
		final int                       limit;
		final int                       bufferSize;
		final Publisher<Object>         upstream;

		volatile Queue<Object> buffer;

		@SuppressWarnings("unused")
		volatile Subscription subscription;
		final static AtomicReferenceFieldUpdater<InnerSubscriber, Subscription> SUBSCRIPTION =
				PlatformDependent.newAtomicReferenceFieldUpdater(InnerSubscriber.class, "subscription");

		boolean done;
		int     outstanding;
		Object  latestValue;

		public InnerSubscriber(Publisher<Object> root, CombineLatestAction<?, V> parent) {
			this.parent = parent;
			this.upstream = root;
			this.limit = parent.bufferSize >> 2;
			this.bufferSize = parent.bufferSize;
		}

		@Override
		public void onSubscribe(Subscription s) {
			super.onSubscribe(s);
			if (!SUBSCRIPTION.compareAndSet(this, null, s)) {
				s.cancel();
				return;
			}
			outstanding = bufferSize;
			s.request(outstanding);
		}

		@Override
		public void onNext(Object t) {
			super.onNext(t);
			if (CombineLatestAction.RUNNING.get(parent) == 0 && CombineLatestAction.RUNNING.compareAndSet(parent,
					0,
					1)) {
				latestValue = t;
				parent.drainLoop(parent.subscribers);
			}
			else {
				Queue<Object> q = buffer;
				if (q == null) {
					q = RingBuffer.newSequencedQueue(RingBuffer.createSingleProducer(bufferSize));
					buffer = q;
				}
				q.add(t);
				parent.drain();
			}
		}

		@Override
		public void onError(Throwable t) {
			super.onError(t);
			if (!done) {
				done = true;
				parent.onError(t);
			}
		}

		@Override
		public void onComplete() {
			if (!done) {
				done = true;
				Queue<?> queue = buffer;
				if ((queue == null || queue.isEmpty()) && CombineLatestAction.WIP_COMPLETED.decrementAndGet(parent) == 0) {
					parent.drain();
				}
			}
		}

		Object readNext() {
			if (latestValue == null) {
				Queue<?> q = buffer;
				if (q != null) {
					latestValue = q.poll();
				}
			}
			return latestValue;
		}

		boolean isTerminated() {
			return done;
		}

		void requestMore() {
			Queue<?> q = buffer;
			latestValue = null;
			if (done) {
				return;
			}

			int r = outstanding - 1;
			if (r > limit) {
				outstanding = r;
				return;
			}
			int k = (bufferSize - r) - (q != null ? q.size() : 0);
			if (k > 0) {
				outstanding = bufferSize;
				SUBSCRIPTION.get(this)
				            .request(k);
			}
		}

		void cancel() {
			Subscription s = SUBSCRIPTION.get(this);
			if (s != SignalType.NOOP_SUBSCRIPTION) {
				s = SUBSCRIPTION.getAndSet(this, SignalType.NOOP_SUBSCRIPTION);
				if (s != SignalType.NOOP_SUBSCRIPTION && s != null) {
					s.cancel();
				}
			}
		}

		@Override
		public Object upstream() {
			return subscription;
		}

		@Override
		public long getCapacity() {
			return parent.getCapacity();
		}


	}

}