/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
public class EmitterProcessor<T> extends BaseProcessor<T, T> {

	final int maxConcurrency;
	final int bufferSize;
	final int limit;

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

	public EmitterProcessor(boolean autoCancel, int maxConcurrency, int bufferSize) {
		super(autoCancel);
		this.maxConcurrency = maxConcurrency;
		this.bufferSize = bufferSize;
		this.limit = Math.max(1, bufferSize / 2);
		this.outstanding = 0;
		SUBSCRIBERS.lazySet(this, EMPTY);
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
			int i;
			for (i = 0; i < innerSubscribers.length; i++) {
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

			int j = getLastIndex(n, inner);
			boolean unbounded = true;

			long seq = -1L;
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
				if(is.unbounded){
					is.actual.onNext(t);
				}
				else {
					long r = is.requested;
					is.unbounded = r == Long.MAX_VALUE;
					Sequence poll = is.unbounded ? null : is.pollCursor;
					if(poll != null){
						if (seq == -1L) {
							seq = buffer(t);
							resetInnerCursors(inner, seq, j, i - 1, n);
						}
					}
					else if (r != 0L) {
						BackpressureUtils.getAndSub(InnerSubscriber.REQUESTED, is, 1);
						is.actual.onNext(t);
					}
					else {
						if (seq == -1L) {
							seq = buffer(t);
							resetInnerCursors(inner, seq, j, i - 1, n);
						}
						is.setPollCursor(seq);
					}
				}

				j++;
				if (j == n) {
					j = 0;
				}
			}
			lastIndex = j;
			lastId = inner[j].id;

			if (!unbounded && RUNNING.getAndIncrement(this) != 0) {
				return;
			}
			if (seq == -1L) {
				requestMore(1L);
			}

			if(!unbounded) {
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
		if (done) {
			throw ReactorFatalException.create(t);
		}
		reportError(t);
		done = true;
		drain();
	}

	@Override
	public void onComplete() {
		if (done) {
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
			long replenishMain = 0;

			int n = inner.length;

			if (n != 0) {
				int j = getLastIndex(n, inner);
				Sequence innerSequence;
				RingBuffer<RingBuffer.Slot<T>> q;

				for (int i = 0; i < n; i++) {

					@SuppressWarnings("unchecked") InnerSubscriber<T> is = (InnerSubscriber<T>) inner[j];

					innerSequence = is.pollCursor;
					q = emitBuffer;

					if (is.done) {
						removeInner(is, autoCancel ? CANCELLED : EMPTY);
						if (autoCancel && subscribers == CANCELLED) {
							cancel();
							return;
						}
						if (innerSequence != null) {
							replenishMain = q.getCursor() - innerSequence.get();
						}
						continue;
					}

					long r = is.requested;

					if (innerSequence == null) {
						if (!d) {
							requestMore(r);
						}
					}


					long _r = innerDrain(is, innerSequence, r, q);

					checkTerminal(is, innerSequence, d, _r);

					if(!d && r == _r){
						requestMore(r - _r);
					}


					j++;
					if (j == n) {
						j = 0;
					}
				}
				lastIndex = j;
				lastId = inner[j].id;
			}

			if (replenishMain != 0L && subscribers != CANCELLED && !d) {
				requestMore(replenishMain);
			}

			missed = RUNNING.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	final long innerDrain(InnerSubscriber<T> is,
			Sequence innerSequence,
			long r,
			RingBuffer<RingBuffer.Slot<T>> q) {

		boolean unbounded = r == Long.MAX_VALUE;

		RingBuffer.Slot<T> o;
		T oo;
		for (; ; ) {

			if (innerSequence == null) {
				break;
			}

			long produced = 0;

			long cursor;
			while (r != 0L) {
				cursor = innerSequence.get() + 1;
				if (q.getCursor() >= cursor) {
					o = q.get(cursor);
					oo = o.value;
				}
				else {
					if(produced != 0){
						break;
					}
					return r;
				}

				if (oo == null) {
					break;
				}

				if (innerSequence.compareAndSet(cursor - 1L, cursor)) {
					is.actual.onNext(oo);
				}
				else {
					if(produced != 0){
						break;
					}
					return r;
				}

				r--;
				produced++;
			}
			if (produced != 0L) {
				if (!unbounded) {
					r = Math.max( 0L, BackpressureUtils.getAndSub(InnerSubscriber.REQUESTED, is, produced) - produced);
				}
				else {
					r = Long.MAX_VALUE;
				}
			}
			if (r == 0 || produced == 0) {
				break;
			}
		}
		return r;
	}

	final void checkTerminal(InnerSubscriber<T> is, Sequence innerSequence, boolean d, long r) {
		if (d) {
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
	}

	final void resetInnerCursors(InnerSubscriber<?>[] inner, long seq, int startIndex, int times, int size) {
		int k = startIndex;
		Sequence poll;
		for (int l = times == -1 ? size : times; l > 0; l--) {
			k--;
			if (k == -1) {
				k = size - 1;
			}

			if (times != -1 && k == startIndex) {
				continue;
			}

			poll = inner[k].pollCursor;
			if (poll == null) {
				inner[k].setPollCursor(seq);
			}
			else if (times != -1) {
				poll.set(seq + 1L);
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
			if(n + 1 > maxConcurrency){
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

		int k;
		if (outstanding != 0) {

			int r = outstanding - (int) n;
			if (r > limit) {
				outstanding = r;
				return;
			}
			k = bufferSize - r;
		}
		else {
			k = bufferSize;
		}

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

		volatile Sequence pollCursor;

		volatile boolean done;

		boolean unbounded = false;

		@SuppressWarnings("unused")
		private volatile long                                    requested = -1L;
		@SuppressWarnings("rawtypes")
		static final     AtomicLongFieldUpdater<InnerSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(InnerSubscriber.class, "requested");

		static final AtomicReferenceFieldUpdater<InnerSubscriber, Sequence> CURSOR = PlatformDependent
				.newAtomicReferenceFieldUpdater(InnerSubscriber.class, "pollCursor");

		public InnerSubscriber(EmitterProcessor<T> parent, final Subscriber<? super T> actual, long id) {
			this.id = id;
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.checkRequest(n, actual)) {
				if (BackpressureUtils.getAndAdd(REQUESTED, this, n) == 0) {
					long demand = n;
					final Sequence poll = pollCursor;
					if (poll != null) {

						final RingBuffer<RingBuffer.Slot<T>> q = parent.emitBuffer;
						InnerSubscriber<?>[] inner = parent.subscribers;

						if (inner.length != 0 && poll.get() < q.getCursor()) {
							while (!done) {
								if (poll.get() == q.getCursor()) {
									break;
								}
								demand = parent.innerDrain(this, poll, demand, parent.emitBuffer);
								if (demand == 0L) {
									break;
								}
							}
						}
					}
				}
				parent.drain();
			}
		}

		public void cancel() {
			done = true;
			parent.drain();
		}

		@Override
		public boolean isExposedToOverflow(Bounded parentPublisher) {
			return parentPublisher.getCapacity() > parent.bufferSize;
		}

		@Override
		public long getCapacity() {
			return parent.bufferSize;
		}

		void setPollCursor(long seq) {
			Sequence pollSequence = Sequencer.newSequence(seq - 1L);
			if(CURSOR.compareAndSet(this, null, pollSequence)){
				parent.emitBuffer.addGatingSequence(pollSequence);
			}
		}

		void start() {
			if (REQUESTED.compareAndSet(this, -1L, 0)) {
				RingBuffer<RingBuffer.Slot<T>> ringBuffer = parent.emitBuffer;
				if (ringBuffer != null) {
					setPollCursor(Math.max(0L, ringBuffer.getMinimumGatingSequence()));
				}
				actual.onSubscribe(this);
			}
		}
	}


}
