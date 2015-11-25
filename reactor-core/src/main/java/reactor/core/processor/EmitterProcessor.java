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

package reactor.core.processor;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.error.CancelException;
import reactor.core.error.InsufficientCapacityException;
import reactor.core.error.ReactorFatalException;
import reactor.core.processor.rb.disruptor.RingBuffer;
import reactor.core.processor.rb.disruptor.Sequence;
import reactor.core.processor.rb.disruptor.Sequencer;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.Bounded;
import reactor.core.support.SignalType;
import reactor.core.support.internal.PlatformDependent;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class EmitterProcessor<T> extends BaseProcessor<T, T> {

	final int maxConcurrency;
	final int bufferSize;
	final int limit;
	final int replay;

	private volatile RingBuffer<RingBuffer.Slot<T>> emitBuffer;

	private volatile boolean done;

	private volatile Throwable error;

	static final InnerSubscriber<?>[] EMPTY = new InnerSubscriber<?>[0];

	static final InnerSubscriber<?>[] CANCELLED = new InnerSubscriber<?>[0];

	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<EmitterProcessor, Throwable> ERROR =
			PlatformDependent.newAtomicReferenceFieldUpdater(EmitterProcessor.class, "error");

	volatile InnerSubscriber<?>[] subscribers;
	@SuppressWarnings("rawtypes")
	static final AtomicReferenceFieldUpdater<EmitterProcessor, InnerSubscriber[]> SUBSCRIBERS =
			PlatformDependent.newAtomicReferenceFieldUpdater(EmitterProcessor.class, "subscribers");

	@SuppressWarnings("unused")
	private volatile int running;
	@SuppressWarnings("rawtypes")
	static final AtomicIntegerFieldUpdater<EmitterProcessor> RUNNING =
			AtomicIntegerFieldUpdater.newUpdater(EmitterProcessor.class, "running");

	long uniqueId;
	long lastId;
	int  lastIndex;
	int  outstanding;

	public EmitterProcessor(boolean autoCancel, int maxConcurrency, int bufferSize, int replayLastN) {
		super(autoCancel);
		this.maxConcurrency = maxConcurrency;
		this.bufferSize = bufferSize;
		this.limit = Math.max(1, bufferSize / 2);
		this.outstanding = 0;
		this.replay = Math.min(replayLastN, bufferSize);
		SUBSCRIBERS.lazySet(this, EMPTY);
		if (replayLastN > 0) {
			getMainQueue();
		}
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		super.subscribe(s);
		InnerSubscriber<T> inner = new InnerSubscriber<T>(this, s, uniqueId++);
		try {
			addInner(inner);
			if (upstreamSubscription != null) {
				inner.start();
			}
		}
		catch (CancelException c) {
			//IGNORE
		}
		catch (Throwable t) {
			removeInner(inner, EMPTY);
			Publishers.<T>error(t).subscribe(s);
		}
	}

	@Override
	protected void doOnSubscribe(Subscription s) {
		InnerSubscriber<?>[] innerSubscribers = subscribers;
		if (innerSubscribers != CANCELLED) {
			for (int i = 0; i < innerSubscribers.length; i++) {
				innerSubscribers[i].start();
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onNext(T t) {
		super.onNext(t);

		InnerSubscriber<?>[] inner = subscribers;
		if (inner == CANCELLED) {
			//FIXME should entorse to the spec and throw CancelException
			return;
		}

		int n = inner.length;
		if (n != 0) {

			long seq = -1L;
			int j = n == 1 ? 0 : getLastIndex(n, inner);
			boolean unbounded = true;

			for (int i = 0; i < n; i++) {

				InnerSubscriber<T> is = (InnerSubscriber<T>) inner[j];

				if (is.done) {
					removeInner(is, autoCancel ? CANCELLED : EMPTY);

					if (autoCancel && subscribers == CANCELLED) {
						if (RUNNING.compareAndSet(this, 0, 1)) {
							cancel();
						}
						return;
					}
					j++;
					if (j == n) {
						j = 0;
					}
					continue;
				}

				unbounded = unbounded && is.unbounded;
				if (is.unbounded && replay == -1) {
					is.actual.onNext(t);
				}
				else {
					long r = is.requested;
					is.unbounded = r == Long.MAX_VALUE;
					Sequence poll = is.unbounded && replay == -1 ? null : is.pollCursor;

					//no tracking and remaining demand positive
					if (r > 0L && poll == null) {
						if (r != Long.MAX_VALUE) {
							is.requested--;
						}
						is.actual.onNext(t);
					}
					//if failed, we buffer if not previously buffered and we assign a tracking cursor to that slot
					else {
						if (seq == -1L) {
							seq = buffer(t);
							if (i > 0) {
								startAllTrackers(inner, seq, j, i - 1, n);
							}

						}
						is.startTracking(seq);
					}
				}

				j++;
				if (j == n) {
					j = 0;
				}
			}
			lastIndex = j;
			lastId = inner[j].id;

			if (seq == -1L) {
				requestMore(1L);
			}

			if (!unbounded) {
				if (RUNNING.getAndIncrement(this) != 0) {
					return;
				}

				drainLoop();
			}
		}
		else {
			buffer(t);
		}
	}

	@Override
	public void onError(Throwable t) {
		super.onError(t);
		if (autoCancel && done) {
			throw ReactorFatalException.create(t);
		}
		reportError(t);
		done = true;
		drain();
	}

	@Override
	public void onComplete() {
		if (autoCancel && done) {
			throw CancelException.get();
		}
		done = true;
		drain();
	}

	@Override
	final public long getAvailableCapacity() {
		return emitBuffer == null ? bufferSize : emitBuffer.remainingCapacity();
	}

	@Override
	final public long getCapacity() {
		return bufferSize;
	}

	RingBuffer<RingBuffer.Slot<T>> getMainQueue() {
		RingBuffer<RingBuffer.Slot<T>> q = emitBuffer;
		if (q == null) {
			q = RingBuffer.createSingleProducer(bufferSize);
			emitBuffer = q;
		}
		return q;
	}

	final long buffer(T value) {
		RingBuffer<RingBuffer.Slot<T>> q = getMainQueue();

		long seq = q.next();

		q.get(seq).value = value;
		q.publish(seq);
		return seq;
	}

	final void drain() {
		if (RUNNING.getAndIncrement(this) == 0) {
			drainLoop();
		}
	}

	final void drainLoop() {
		int missed = 1;
		for (; ; ) {
			InnerSubscriber<?>[] inner = subscribers;
			if (inner == CANCELLED) {
				cancel();
				return;
			}
			boolean d = done;

			if (d && inner == EMPTY) {
				return;
			}

			int n = inner.length;

			if (n != 0) {
				int j = getLastIndex(n, inner);
				Sequence innerSequence;
				RingBuffer<RingBuffer.Slot<T>> q = null;
				long _r;
				long drained;

				if (q == null) {
					q = emitBuffer;
					drained = -1L;
				}
				else {
					drained = 0L;
				}

				for (int i = 0; i < n; i++) {
					@SuppressWarnings("unchecked") InnerSubscriber<T> is = (InnerSubscriber<T>) inner[j];

					long r = is.requested;

					if (is.done) {
						removeInner(is, autoCancel ? CANCELLED : EMPTY);
						if (autoCancel && subscribers == CANCELLED) {
							cancel();
							return;
						}
						continue;
					}

					innerSequence = is.pollCursor;

					if (innerSequence != null && r > 0 ) {
						_r = r;

						boolean unbounded = _r == Long.MAX_VALUE;
						T oo;
						long cursor;
						while (_r != 0L) {
							cursor = innerSequence.get() + 1;
							if (q.getCursor() >= cursor) {
								oo = q.get(cursor).value;
							}
							else {
								break;
							}

							innerSequence.set(cursor);
							is.actual.onNext(oo);
							if(!unbounded) {
								_r--;
							}
						}

						if (r > _r) {
							BackpressureUtils.getAndSub(InnerSubscriber.REQUESTED, is, r - _r);
						}

						drained = n == 1 || drained == -1L ? ( r - _r ) : Math.min(drained, r - _r);
					}
					else {
						_r = 0L;
					}

					if (d) {
						checkTerminal(is, innerSequence, _r);
					}

					j++;
					if (j == n) {
						j = 0;
					}
				}
				lastIndex = j;
				lastId = inner[j].id;

				if (!d && subscribers != CANCELLED) {
					if(outstanding == 0){
						outstanding = bufferSize;
						Subscription s = upstreamSubscription;
						if(s != null){
							s.request(bufferSize);
						}
					}
					else if (drained > 0L) {
						requestMore(n != 1 ? Math.min(q.remainingCapacity(), drained) : drained);
					}
				}
			}

			missed = RUNNING.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	final void checkTerminal(InnerSubscriber<T> is, Sequence innerSequence, long r) {
		Throwable e = error;
		if ((e != null && r == 0) || innerSequence == null || innerSequence.get() >= emitBuffer.getCursor()) {
			removeInner(is, EMPTY);
			if (!is.done) {
				if (e == null) {
					is.actual.onComplete();
				}
				else {
					is.actual.onError(e);
				}
			}
		}
	}

	final void startAllTrackers(InnerSubscriber<?>[] inner, long seq, int startIndex, int times, int size) {
		int k = startIndex;
		Sequence poll;
		for (int l = times; l > 0; l--) {
			k--;
			if (k == -1) {
				k = size - 1;
			}

			if (k == startIndex) {
				continue;
			}

			poll = inner[k].pollCursor;
			if (poll == null) {
				inner[k].startTracking(seq);
			}
		}
	}

	final void reportError(Throwable t) {
		ERROR.compareAndSet(this, null, t);
	}

	final void addInner(InnerSubscriber<T> inner) {
		for (; ; ) {
			InnerSubscriber<?>[] a = subscribers;
			if (a == CANCELLED) {
				Publishers.<T>empty().subscribe(inner.actual);
			}
			int n = a.length;
			if (n + 1 > maxConcurrency) {
				throw InsufficientCapacityException.get();
			}
			InnerSubscriber<?>[] b = new InnerSubscriber[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = inner;
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	final int getLastIndex(int n, InnerSubscriber<?>[] inner) {
		int index = lastIndex;
		long startId = lastId;
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
		return index;
	}

	final void removeInner(InnerSubscriber<?> inner, InnerSubscriber<?>[] lastRemoved) {
		for (; ; ) {
			InnerSubscriber<?>[] a = subscribers;
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
			InnerSubscriber<?>[] b;
			if (n == 1) {
				b = lastRemoved;
			}
			else {
				b = new InnerSubscriber<?>[n - 1];
				System.arraycopy(a, 0, b, 0, j);
				System.arraycopy(a, j + 1, b, j, n - j - 1);
			}
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				Sequence poll = inner.pollCursor;
				if (poll != null) {
					getMainQueue().removeGatingSequence(poll);
				}
				return;
			}
		}
	}

	final void requestMore(long n) {
		Subscription subscription = upstreamSubscription;
		if (subscription == SignalType.NOOP_SUBSCRIPTION) {
			return;
		}

		//System.out.println(Thread.currentThread() + " " + n + "/" + outstanding + " " + bufferSize);
		int k;
		int r = outstanding - (int) n;
		if (r > limit) {
			outstanding = r;
			return;
		}
		k = bufferSize - r;

		outstanding = bufferSize;
		if (subscription != null && k > 0) {
			subscription.request(k);
		}
	}

	final void cancel() {
		if (!done) {
			Subscription s = upstreamSubscription;
			if (s != null) {
				upstreamSubscription = null;
				s.cancel();
			}
		}
	}

	static final class InnerSubscriber<T> implements Subscription, Bounded {

		final long                  id;
		final EmitterProcessor<T>   parent;
		final Subscriber<? super T> actual;

		volatile boolean done;

		boolean unbounded = false;

		@SuppressWarnings("unused")
		private volatile long requested = -1L;

		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<InnerSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(InnerSubscriber.class, "requested");

		volatile Sequence pollCursor;

		static final AtomicReferenceFieldUpdater<InnerSubscriber, Sequence> CURSOR =
				PlatformDependent.newAtomicReferenceFieldUpdater(InnerSubscriber.class, "pollCursor");

		public InnerSubscriber(EmitterProcessor<T> parent, final Subscriber<? super T> actual, long id) {
			this.id = id;
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.checkRequest(n, actual)) {
				BackpressureUtils.getAndAdd(REQUESTED, this, n);
				if (EmitterProcessor.RUNNING.getAndIncrement(parent) == 0) {
					parent.drainLoop();
				}
			}
		}

		public void cancel() {
			done = true;
			parent.drain();
		}

		@Override
		public boolean isExposedToOverflow(Bounded parentPublisher) {
			return false;
		}

		@Override
		public long getCapacity() {
			return parent.bufferSize;
		}

		void startTracking(long seq) {
			Sequence pollSequence = Sequencer.newSequence(seq - 1L);
			if (CURSOR.compareAndSet(this, null, pollSequence)) {
				parent.emitBuffer.addGatingSequence(pollSequence);
			}
		}

		void start() {
			if (REQUESTED.compareAndSet(this, -1L, 0)) {
				RingBuffer<RingBuffer.Slot<T>> ringBuffer = parent.emitBuffer;
				if (ringBuffer != null) {
					if (parent.replay > 0) {
						long cursor = ringBuffer.getCursor();
						startTracking(Math.max(0L, cursor - Math.min(parent.replay, cursor % ringBuffer
								.getBufferSize())));
					}
					else {
						startTracking(Math.max(0L, ringBuffer.getMinimumGatingSequence()));
					}
				}

				actual.onSubscribe(this);
			}
		}
	}


}
