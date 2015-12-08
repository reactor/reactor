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

package reactor.core.publisher.operator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
import reactor.fn.BiFunction;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.fn.tuple.Tuple;

/**
 * A zip operator to combine 1 by 1 each upstream in parallel given the combinator function.
 * @author Stephane Maldini
 * @since 2.1
 */
public final class ZipOperator<TUPLE extends Tuple, V>
		implements Function<Subscriber<? super V>, Subscriber<? super Publisher[]>> {

	/**
	 *
	 */
	public static final Function JOIN_FUNCTION = new Function<Tuple, List>() {
		@Override
		public List<?> apply(Tuple ts) {
			return Arrays.asList(ts.toArray());
		}
	};

	/**
	 *
	 */
	public static final BiFunction JOIN_BIFUNCTION = new BiFunction<Object, Object, List>() {
		@Override
		public List<?> apply(Object t1, Object t2) {
			return Arrays.asList(t1, t2);
		}
	};

	final Function<? super TUPLE, ? extends V> combinator;
	final int                                  bufferSize;

	public ZipOperator(final Function<? super TUPLE, ? extends V> combinator, int bufferSize) {
		this.combinator = combinator;
		this.bufferSize = bufferSize;
	}

	@Override
	public Subscriber<? super Publisher[]> apply(Subscriber<? super V> t) {
		return new ZipBarrier<>(t, combinator, bufferSize);
	}

	static final class ZipBarrier<TUPLE extends Tuple, V> extends SubscriberWithDemand<Publisher[], V>
			implements ReactiveState.LinkedUpstreams {

		final Function<? super TUPLE, ? extends V> combinator;
		final int                                  bufferSize;
		final int                                  limit;

		@SuppressWarnings("unused")
		private volatile Throwable error;

		private volatile boolean cancelled;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<ZipBarrier, Throwable> ERROR =
				PlatformDependent.newAtomicReferenceFieldUpdater(ZipBarrier.class, "error");

		private ZipState<?>[] subscribers;

		@SuppressWarnings("unused")
		private volatile int running;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<ZipBarrier> RUNNING =
				AtomicIntegerFieldUpdater.newUpdater(ZipBarrier.class, "running");

		Object[] valueCache;

		public ZipBarrier(Subscriber<? super V> actual,
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

				if (sources.length == 0) {
					subscriber.onComplete();
					return;
				}

				ZipState[] subscribers = new ZipState[sources.length];
				valueCache = new Object[sources.length];

				int i;
				ZipState<?> inner;
				Publisher pub;
				for (i = 0; i < sources.length; i++) {
					pub = sources[i];
					if (pub instanceof Supplier) {
						inner = new ScalarState(((Supplier<?>) pub).get());
						subscribers[i] = inner;
					}
					else {
						inner = new BufferSubscriber(this);
						subscribers[i] = inner;
					}
				}

				this.subscribers = subscribers;

				for (i = 0; i < sources.length; i++) {
					subscribers[i].subscribeTo(sources[i]);
				}

				drain();
			}
		}

		@Override
		protected void doRequest(long n) {
			BackpressureUtils.getAndAdd(REQUESTED, this, n);
			drain();
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
			ZipState[] s = subscribers;
			if (s == null || s.length == 0) {
				if (RUNNING.getAndIncrement(this) == 0) {
					subscriber.onComplete();
				}
			}
		}

		@Override
		public boolean isStarted() {
			return subscribers != null;
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

		@Override
		public Iterator<?> upstreams() {
			return Arrays.asList(subscribers).iterator();
		}

		@Override
		public long upstreamsCount() {
			return subscribers.length;
		}

		void drain() {
			ZipState[] subscribers = this.subscribers;
			if (subscribers == null) {
				return;
			}
			if (RUNNING.getAndIncrement(this) == 0) {
				drainLoop(subscribers);
			}
		}

		@SuppressWarnings("unchecked")
		void drainLoop(ZipState[] inner) {

			final Subscriber<? super V> actual = this.subscriber;
			int missed = 1;
			for (; ; ) {
				if (checkImmediateTerminate()) {
					return;
				}

				int n = inner.length;
				int replenishMain = 0;
				long r = requestedFromDownstream();

				ZipState<?> state;

				for (; ; ) {

					final Object[] tuple = valueCache;
					boolean completeTuple = true;
					int i;
					for (i = 0; i < n; i++) {
						state = inner[i];

						Object next = state.readNext();

						if (next == null) {
							if (state.isTerminated()) {
								actual.onComplete();
								cancelStates();
								return;
							}

							completeTuple = false;
							continue;
						}

						if(r != 0) {
							tuple[i] = next;
						}
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
							Exceptions.throwIfFatal(e);
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

							if (state.readNext() == null && state.isTerminated()) {
								actual.onComplete();
								cancelStates();
								return;
							}
						}

						valueCache = new Object[n];
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
			ZipState[] inner = subscribers;
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

		@Override
		public boolean isTerminated() {
			if(subscribers == null) return super.isTerminated();
			for(int i = 0; i < subscribers.length; i++){
				if(!subscribers[i].isTerminated()){
					return false;
				}
			}
			return true;
		}
	}

	interface ZipState<V> extends Subscriber<Object>,
	                              ReactiveState.ActiveDownstream,
	                              ReactiveState.Buffering,
	                              ReactiveState.ActiveUpstream {

		V readNext();

		boolean isTerminated();

		void requestMore();

		void cancel();

		void subscribeTo(Publisher<?> o);
	}

	static final class ScalarState implements ZipState<Object> {

		final Object val;

		boolean read = false;

		ScalarState(Object val) {
			this.val = val;
		}

		@Override
		public Object readNext() {
			return read ? null : val;
		}

		@Override
		public boolean isTerminated() {
			return read;
		}

		@Override
		public boolean isCancelled() {
			return read;
		}

		@Override
		public boolean isStarted() {
			return !read;
		}

		@Override
		public void onSubscribe(Subscription s) {
			//IGNORE
		}

		@Override
		public void onNext(Object o) {
			//IGNORE
		}

		@Override
		public void onError(Throwable t) {
			//IGNORE
		}

		@Override
		public void onComplete() {
			//IGNORE
		}

		@Override
		public void subscribeTo(Publisher<?> o) {
			//IGNORE
		}

		@Override
		public void requestMore() {
			read = true;
		}

		@Override
		public void cancel() {
			read = true;
		}

		@Override
		public long pending() {
			return read ? 0L : 1L;
		}

		@Override
		public long getCapacity() {
			return 1L;
		}

		@Override
		public String toString() {
			return "ScalarState{" +
					"read=" + read +
					", val=" + val +
					'}';
		}
	}

	static final class BufferSubscriber<V> extends BaseSubscriber<Object> implements ReactiveState.Bounded, ZipState<Object>,
	                                                                                 ReactiveState.Upstream,
	                                                                                 ReactiveState.ActiveUpstream,
	                                                                                 ReactiveState.Downstream {

		final ZipBarrier<?, V> parent;
		final Queue<Object>    queue;
		final int              limit;
		final int              bufferSize;

		@SuppressWarnings("unused")
		volatile Subscription subscription;
		final static AtomicReferenceFieldUpdater<BufferSubscriber, Subscription> SUBSCRIPTION =
				PlatformDependent.newAtomicReferenceFieldUpdater(BufferSubscriber.class, "subscription");

		volatile boolean done;
		int outstanding;

		public BufferSubscriber(ZipBarrier<?, V> parent) {
			this.parent = parent;
			this.bufferSize = parent.bufferSize;
			this.limit = bufferSize >> 2;
			this.queue = RingBuffer.newSequencedQueue(RingBuffer.createSingleProducer(bufferSize));
		}

		@Override
		public Object upstream() {
			return subscription;
		}

		@Override
		public boolean isCancelled() {
			return parent.isCancelled();
		}

		@Override
		public long pending() {
			return queue.size();
		}

		@Override
		public Subscriber<? super Publisher[]> downstream() {
			return parent;
		}

		@Override
		public void subscribeTo(Publisher<?> o) {
			o.subscribe(this);
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
		public void onNext(Object x) {
			super.onNext(x);
			queue.add(x);
			try {
				parent.drain();
			}
			catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				parent.doOnSubscriberError(t);
			}
		}

		@Override
		public void onError(Throwable t) {
			super.onError(t);
			parent.doOnSubscriberError(t);
		}

		@Override
		public void onComplete() {
			done = true;
			parent.drain();
		}

		@Override
		public Object readNext() {
			return queue.peek();
		}

		@Override
		public boolean isTerminated() {
			return done && queue.isEmpty();
		}

		@Override
		public boolean isStarted() {
			return parent.isStarted();
		}

		@Override
		public void requestMore() {
			queue.poll();
			int r = outstanding - 1;
			if (r > limit) {
				outstanding = r;
				return;
			}
			outstanding = bufferSize;
			int k = bufferSize - r;
			if (k > 0) {
				SUBSCRIPTION.get(this)
				            .request(k);
			}
		}

		@Override
		public void cancel() {
			Subscription s = SUBSCRIPTION.get(this);
			if (s != SignalType.NOOP_SUBSCRIPTION) {
				s = SUBSCRIPTION.getAndSet(this, SignalType.NOOP_SUBSCRIPTION);
				if (s != SignalType.NOOP_SUBSCRIPTION && s != null) {
					s.cancel();
				}
			}
		}

		@Override
		public long getCapacity() {
			return bufferSize;
		}

		@Override
		public String toString() {
			return "BufferSubscriber{" +
					"queue=" + queue +
					", bufferSize=" + bufferSize +
					", done=" + done +
					", outstanding=" + outstanding +
					", subscription=" + subscription +
					'}';
		}
	}

}