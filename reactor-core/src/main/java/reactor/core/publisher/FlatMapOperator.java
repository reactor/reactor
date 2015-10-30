/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Vnless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.processor.rb.disruptor.RingBuffer;
import reactor.core.processor.rb.disruptor.Sequence;
import reactor.core.processor.rb.disruptor.Sequencer;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.Bounded;
import reactor.core.support.SignalType;
import reactor.core.support.internal.PlatformDependent;
import reactor.fn.Function;
import reactor.fn.Supplier;

/**
 * A merge operator that transforms input T to Publisher of V using the assigned map function.
 * Once transformed, publishers are then subscribed and requested with the following demand rules:
 * - request eagerly up to bufferSize
 * - downstream consume buffer once all inner subscribers have read
 * - when more than half of the demand has been fulfilled, request more
 * - request to upstream represent the maximum concurrent merge possible
 * <p>
 * Original design idea from David Karnok (RxJava 2.0)
 *
 * @author David Karnok
 * @author Stephane Maldini
 * @since 2.1
 */
public final class FlatMapOperator<T, V> implements Function<Subscriber<? super V>, Subscriber<? super T>> {

	final Function<? super T, ? extends Publisher<? extends V>> mapper;
	final int                                                   maxConcurrency;
	final int                                                   bufferSize;

	public FlatMapOperator(
	  Function<? super T, ? extends Publisher<? extends V>> mapper, int maxConcurrency, int bufferSize) {
		this.mapper = mapper;
		this.maxConcurrency = maxConcurrency;
		this.bufferSize = bufferSize;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super V> t) {
		return new MergeBarrier<>(t, mapper, maxConcurrency, bufferSize);
	}

	static final class MergeBarrier<T, V> extends SubscriberBarrier<T, V> {

		final Function<? super T, ? extends Publisher<? extends V>> mapper;
		final int                                                   maxConcurrency;
		final int                                                   bufferSize;
		final int                                                   limit;

		private Sequence pollCursor;
		private volatile RingBuffer<RingBuffer.Slot<V>> emitBuffer;

		private volatile boolean done;

		private volatile Throwable error;

		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeBarrier, Throwable> ERROR =
		  PlatformDependent.newAtomicReferenceFieldUpdater(MergeBarrier.class, "error");

		private volatile boolean cancelled;

		volatile InnerSubscriber<?, ?>[] subscribers;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeBarrier, InnerSubscriber[]> SUBSCRIBERS =
		  PlatformDependent.newAtomicReferenceFieldUpdater(MergeBarrier.class, "subscribers");

		@SuppressWarnings("unused")
		private volatile int running;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MergeBarrier> RUNNING =
		  AtomicIntegerFieldUpdater.newUpdater(MergeBarrier.class, "running");

		static final InnerSubscriber<?, ?>[] EMPTY = new InnerSubscriber<?, ?>[0];

		static final InnerSubscriber<?, ?>[] CANCELLED = new InnerSubscriber<?, ?>[0];

		private volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<MergeBarrier> REQUESTED =
		  AtomicLongFieldUpdater.newUpdater(MergeBarrier.class, "requested");


		long lastRequest;
		long uniqueId;
		long lastId;
		int  lastIndex;

		public MergeBarrier(Subscriber<? super V> actual, Function<? super T, ? extends Publisher<? extends V>>
		  mapper, int maxConcurrency, int bufferSize) {
			super(actual);
			this.mapper = mapper;
			this.maxConcurrency = maxConcurrency;
			this.bufferSize = bufferSize;
			this.limit = Math.max(1, maxConcurrency / 2);
			SUBSCRIBERS.lazySet(this, EMPTY);
		}

		@Override
		protected void doOnSubscribe(Subscription s) {
			subscriber.onSubscribe(this);
			if (!cancelled) {
				if (maxConcurrency == Integer.MAX_VALUE) {
					subscription.request(Long.MAX_VALUE);
				} else {
					subscription.request(maxConcurrency);
				}
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		protected void doNext(T t) {
			// safeguard against misbehaving sources
			final Publisher<? extends V> p = mapper.apply(t);

			if (p instanceof Supplier) {
				V v = ((Supplier<? extends V>) p).get();
				if (v != null) {
					tryEmit(v);
					return;
				}
			}


			InnerSubscriber<T, V> inner = new InnerSubscriber<>(this, uniqueId++);
			addInner(inner);
			p.subscribe(inner);
		}

		void addInner(InnerSubscriber<T, V> inner) {
			for (; ; ) {
				InnerSubscriber<?, ?>[] a = subscribers;
				if (a == CANCELLED) {
					inner.cancel();
					return;
				}
				int n = a.length;
				InnerSubscriber<?, ?>[] b = new InnerSubscriber[n + 1];
				System.arraycopy(a, 0, b, 0, n);
				b[n] = inner;
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					return;
				}
			}
		}

		void removeInner(InnerSubscriber<T, V> inner) {
			for (; ; ) {
				InnerSubscriber<?, ?>[] a = subscribers;
				if (a == CANCELLED || a == EMPTY) {
					return;
				}
				int n = a.length;
				int j = -1;
				for (int i = 0; i < n; i++) {
					if (a[i] == inner) {
						j = i;
						break;
					}
				}
				if (j < 0) {
					return;
				}
				InnerSubscriber<?, ?>[] b;
				if (n == 1) {
					b = EMPTY;
				} else {
					b = new InnerSubscriber<?, ?>[n - 1];
					System.arraycopy(a, 0, b, 0, j);
					System.arraycopy(a, j + 1, b, j, n - j - 1);
				}
				if (SUBSCRIBERS.compareAndSet(this, a, b)) {
					return;
				}
			}
		}

		@SuppressWarnings("unchecked")
		RingBuffer<RingBuffer.Slot<V>> getMainQueue() {
			RingBuffer<RingBuffer.Slot<V>> q = emitBuffer;
			if (q == null) {
				q = RingBuffer.createSingleProducer(
				  maxConcurrency == Integer.MAX_VALUE ? bufferSize : maxConcurrency
				);
				q.addGatingSequences(pollCursor = Sequencer.newSequence(-1L));
				emitBuffer = q;
			}
			return q;
		}

		void tryEmit(V value) {
			if (RUNNING.get(this) == 0 && RUNNING.compareAndSet(this, 0, 1)) {
				long r = requested;
				if (r != 0L) {
					if (null != value) {
						subscriber.onNext(value);
					}
					if (r != Long.MAX_VALUE) {
						REQUESTED.decrementAndGet(this);
					}
					if (maxConcurrency != Integer.MAX_VALUE && !cancelled
					  && ++lastRequest == limit) {
						lastRequest = 0;
						subscription.request(limit);
					}
				} else {
					RingBuffer<RingBuffer.Slot<V>> q = getMainQueue();
					long seq = q.tryNext();
					q.get(seq).value = value;
					q.publish(seq);

				}
				if (RUNNING.decrementAndGet(this) == 0) {
					return;
				}
			} else {
				RingBuffer<RingBuffer.Slot<V>> q = getMainQueue();
				long seq = q.tryNext();
				q.get(seq).value = value;
				q.publish(seq);
				if (RUNNING.getAndIncrement(this) != 0) {
					return;
				}
			}
			drainLoop();
		}

		@SuppressWarnings("unchecked")
		RingBuffer<RingBuffer.Slot<V>> getInnerQueue(InnerSubscriber<T, V> inner) {
			RingBuffer<RingBuffer.Slot<V>> q = inner.queue;
			if (q == null) {
				q = RingBuffer.createSingleProducer(bufferSize);
				q.addGatingSequences(inner.pollCursor = Sequencer.newSequence(-1L));
				inner.queue = q;
			}
			return q;
		}

		void tryEmit(V value, InnerSubscriber<T, V> inner) {
			if (RUNNING.get(this) == 0 && RUNNING.compareAndSet(this, 0, 1)) {
				long r = requested;
				if (r != 0L) {
					subscriber.onNext(value);
					if (r != Long.MAX_VALUE) {
						REQUESTED.decrementAndGet(this);
					}
					inner.requestMore(1);
				} else {
					RingBuffer<RingBuffer.Slot<V>> q = getInnerQueue(inner);
					long seq = q.tryNext();
					q.get(seq).value = value;
					q.publish(seq);
				}
				if (RUNNING.decrementAndGet(this) == 0) {
					return;
				}
			} else {
				RingBuffer<RingBuffer.Slot<V>> q = getInnerQueue(inner);
				long seq = q.tryNext();
				q.get(seq).value = value;
				q.publish(seq);
				if (RUNNING.getAndIncrement(this) != 0) {
					return;
				}
			}
			drainLoop();
		}

		@Override
		protected void doError(Throwable t) {
			if (done) {
				throw CancelException.get();
			}
			reportError(t);
			done = true;
			drain();
		}

		@Override
		protected void doComplete() {
			if (done) {
				throw CancelException.get();
			}
			done = true;
			drain();
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
					unsubscribe();
				}
			}
		}

		void reportError(Throwable t) {
			if(!ERROR.compareAndSet(this, null, t)){
				throw ReactorFatalException.create(t);
			}
		}

		void drain() {
			if (RUNNING.getAndIncrement(this) == 0) {
				drainLoop();
			}
		}

		void drainLoop() {
			final Subscriber<? super V> child = this.subscriber;
			int missed = 1;
			for (; ; ) {
				if (checkTerminate()) {
					return;
				}
				RingBuffer<RingBuffer.Slot<V>> svq = emitBuffer;

				long r = requested;
				boolean unbounded = r == Long.MAX_VALUE;

				long replenishMain = 0;

				if (svq != null) {
					for (; ; ) {
						long scalarEmission = 0;
						RingBuffer.Slot<V> o;
						V oo = null;
						while (r != 0L) {
							long cursor = pollCursor.get() + 1;

							if (svq.getCursor() >= cursor) {
								o = svq.get(cursor);
								oo = o.value;
							} else {
								o = null;
								oo = null;
							}

							if (checkTerminate()) {
								return;
							}
							if (oo == null) {
								break;
							}

							o.value = null;
							pollCursor.set(cursor);
							child.onNext(oo);

							replenishMain++;
							scalarEmission++;
							r--;
						}
						if (scalarEmission != 0L) {
							if (unbounded) {
								r = Long.MAX_VALUE;
							} else {
								r = REQUESTED.addAndGet(this, -scalarEmission);
							}
						}
						if (r == 0L || oo == null) {
							break;
						}
					}
				}

				boolean d = done;
				svq = emitBuffer;
				InnerSubscriber<?, ?>[] inner = subscribers;
				int n = inner.length;

				if (d && (svq == null || svq.pending() == 0) && n == 0) {
					Throwable e = error;
					if (e == null) {
						child.onComplete();
					} else {
						subscriber.onError(e);
					}
					return;
				}

				boolean innerCompleted = false;
				if (n != 0) {
					long startId = lastId;
					int index = lastIndex;

					if (n <= index || inner[index].id != startId) {
						if (n <= index) {
							index = 0;
						}
						int j = index;
						for (int i = 0; i < n; i++) {
							if (inner[j].id == startId) {
								break;
							}
							j++;
							if (j == n) {
								j = 0;
							}
						}
						index = j;
						lastIndex = j;
						lastId = inner[j].id;
					}

					int j = index;
					for (int i = 0; i < n; i++) {
						if (checkTerminate()) {
							return;
						}
						@SuppressWarnings("unchecked")
						InnerSubscriber<T, V> is = (InnerSubscriber<T, V>) inner[j];

						RingBuffer.Slot<V> o;
						V oo = null;
						for (; ; ) {
							long produced = 0;
							while (r != 0L) {
								if (checkTerminate()) {
									return;
								}
								RingBuffer<RingBuffer.Slot<V>> q = is.queue;
								if (q == null) {
									break;
								}
								long cursor = is.pollCursor.get() + 1;

								if (q.getCursor() >= cursor) {
									o = q.get(cursor);
									oo = o.value;
								} else {
									o = null;
									oo = null;
								}

								if (oo == null) {
									break;
								}

								o.value = null;
								is.pollCursor.set(cursor);
								child.onNext(oo);

								r--;
								produced++;
							}
							if (produced != 0L) {
								if (!unbounded) {
									r = REQUESTED.addAndGet(this, -produced);
								} else {
									r = Long.MAX_VALUE;
								}
								is.requestMore(produced);
							}
							if (r == 0 || oo == null) {
								break;
							}
						}
						boolean innerDone = is.done;
						RingBuffer<RingBuffer.Slot<V>> innerQueue = is.queue;
						if (innerDone && (innerQueue == null || innerQueue.pending() == 0)) {
							removeInner(is);
							if (checkTerminate()) {
								return;
							}
							replenishMain++;
							innerCompleted = true;
						}
						if (r == 0L) {
							break;
						}

						j++;
						if (j == n) {
							j = 0;
						}
					}
					lastIndex = j;
					lastId = inner[j].id;
				}

				if (replenishMain != 0L && !cancelled) {
					subscription.request(maxConcurrency == 1 ? 1 : replenishMain);
				}
				if (innerCompleted) {
					continue;
				}
				missed = RUNNING.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		boolean checkTerminate() {
			if (cancelled) {
				subscription.cancel();
				unsubscribe();
				return true;
			}
			Throwable e = error;
			if (e != null) {
				try {
					subscriber.onError(error);
				} finally {
					unsubscribe();
				}
				return true;
			}
			return false;
		}

		void unsubscribe() {
			InnerSubscriber<?, ?>[] a = subscribers;
			if (a != CANCELLED) {
				a = SUBSCRIBERS.getAndSet(this, CANCELLED);
				if (a != CANCELLED) {
					for (InnerSubscriber<?, ?> inner : a) {
						inner.cancel();
					}
				}
			}
		}

	}

	static final class InnerSubscriber<T, V>
	  extends BaseSubscriber<V> implements Bounded {
		final long               id;
		final MergeBarrier<T, V> parent;
		final int                limit;
		final int                bufferSize;

		@SuppressWarnings("unused")
		volatile Subscription subscription;
		final static AtomicReferenceFieldUpdater<InnerSubscriber, Subscription> SUBSCRIPTION =
		  PlatformDependent.newAtomicReferenceFieldUpdater(InnerSubscriber.class, "subscription");

		Sequence pollCursor;

		volatile boolean                done;
		volatile RingBuffer<RingBuffer.Slot<V>> queue;
		int outstanding;

		public InnerSubscriber(MergeBarrier<T, V> parent, long id) {
			this.id = id;
			this.parent = parent;
			this.bufferSize = parent.bufferSize;
			this.limit = bufferSize >> 2;
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
		public void onNext(V t) {
			super.onNext(t);
			parent.tryEmit(t, this);
		}

		@Override
		public void onError(Throwable t) {
			super.onError(t);
			parent.reportError(t);
			done = true;
			parent.drain();
		}

		@Override
		public void onComplete() {
			done = true;
			parent.drain();
		}

		void requestMore(long n) {
			int r = outstanding - (int) n;
			if (r > limit) {
				outstanding = r;
				return;
			}
			outstanding = bufferSize;
			int k = bufferSize - r;
			if (k > 0) {
				SUBSCRIPTION.get(this).request(k);
			}
		}

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
		public boolean isExposedToOverflow(Bounded parentPublisher) {
			return parentPublisher.getCapacity() > bufferSize;
		}

		@Override
		public long getCapacity() {
			return bufferSize;
		}
	}

}
