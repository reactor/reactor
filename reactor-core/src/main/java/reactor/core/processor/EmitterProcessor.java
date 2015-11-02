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
		this.limit = Math.max(1, maxConcurrency / 2);
		this.outstanding = 0;
		SUBSCRIBERS.lazySet(this, EMPTY);
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		super.subscribe(s);
		InnerSubscriber<T> inner = new InnerSubscriber<T>(this, s, uniqueId++);
		try {
			addInner(inner);
			if(upstreamSubscription != null){
				inner.start();
			}
		}
		catch (CancelException c){
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

			if(n == 1 && inner[0].unbounded){
				((InnerSubscriber<T>)inner[0]).actual.onNext(t);
				return;
			}

			int j = getLastIndex(n, inner);

			if (RUNNING.get(this) == 0 && RUNNING.compareAndSet(this, 0, 1)) {
				long seq = -1L;
				for (int i = 0; i < n; i++) {

					InnerSubscriber<T> is = (InnerSubscriber<T>) inner[j];

					if (is.done) {
						removeInner(is, autoCancel ? CANCELLED : EMPTY);

						if(autoCancel && subscribers == CANCELLED){
							cancel();
							return;
						}
						continue;
					}

					long r = is.requested;
					is.unbounded = r == Long.MAX_VALUE;
					if (r != 0L) {
						BackpressureUtils.getAndSub(InnerSubscriber.REQUESTED, is, 1);
						is.actual.onNext(t);
						if (seq != -1L){
							is.forcePollCursor(seq);
						}
						requestMore(1L);
					}
					else {
						if (seq == -1L) {
							seq = buffer(t);
							if(i > 0){
								int k = j;
								for(int l = i; l > 0; l--){
									k--;
									if (k == -1) {
										k = n - 1;
									}
									inner[k].forcePollCursor(seq);
								}
							}
						}
						is.setPollCursor(seq);
					}

					j++;
					if (j == n) {
						j = 0;
					}
				}
				lastIndex = j;
				lastId = inner[j].id;

				if (RUNNING.decrementAndGet(this) == 0) {
					return;
				}
			}
			else {
				buffer(t);

				if (RUNNING.getAndIncrement(this) != 0) {
					return;
				}
			}
			drainLoop();
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
	public long getAvailableCapacity() {
		return emitBuffer == null ? bufferSize : emitBuffer.remainingCapacity();
	}

	@Override
	public long getCapacity() {
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

	long buffer(T value) {
		RingBuffer<RingBuffer.Slot<T>> q = getMainQueue();

		long seq = q.tryNext();

		q.get(seq).value = value;
		q.publish(seq);
		return seq;
	}

	void drain() {
		if (RUNNING.getAndIncrement(this) == 0) {
			drainLoop();
		}
	}

	void drainLoop() {
		int missed = 1;
		for (; ; ) {
			InnerSubscriber<?>[] inner = subscribers;
			if (inner == CANCELLED) {
				cancel();
				return;
			}

			long replenishMain = 0;

			boolean d = done;
			int n = inner.length;

			boolean innerCompleted = false;
			if (n != 0) {
				int j = getLastIndex(n, inner);

				for (int i = 0; i < n; i++) {

					@SuppressWarnings("unchecked") InnerSubscriber<T> is = (InnerSubscriber<T>) inner[j];

					long r = is.requested;
					boolean unbounded = r == Long.MAX_VALUE;

					Sequence innerSequence = is.pollCursor;
					RingBuffer<RingBuffer.Slot<T>> q = emitBuffer;

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

					RingBuffer.Slot<T> o;
					T oo = null;
					for (; ; ) {

						if(innerSequence == null){
							if(!d) {
								requestMore(r);
							}
							break;
						}

						long produced = 0;

						long cursor = -1L;
						while (r > 0L) {
							cursor = innerSequence.get() + 1;
							if (q.getCursor() >= cursor) {
								o = q.get(cursor);
								oo = o.value;
							}
							else {
								oo = null;
							}

							if (oo == null) {
								break;
							}

							is.actual.onNext(oo);

							r--;
							produced++;
						}
						if(cursor != -1L) {
							innerSequence.set(cursor);
						}
						if (produced != 0L) {
							if (!unbounded) {
								r = InnerSubscriber.REQUESTED.addAndGet(is, -produced);
							}
							else {
								r = Long.MAX_VALUE;
							}
							if(!d) {
								requestMore(produced);
							}
						}
						if (r == 0 || oo == null) {
							break;
						}
					}

					if (d) {
						Throwable e = error;
						if((e != null && r == 0)
								|| innerSequence == null || innerSequence.get() >= emitBuffer.getCursor()) {
							removeInner(is, EMPTY);
							if(!is.done) {
								if (e == null) {
									is.actual.onComplete();
								}
								else {
									is.actual.onError(e);
								}
							}

							innerCompleted = true;
						}
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
			if (innerCompleted) {
				continue;
			}
			missed = RUNNING.addAndGet(this, -missed);
			if (missed == 0) {
				break;
			}
		}
	}

	void reportError(Throwable t) {
		ERROR.compareAndSet(this, null, t);
	}

	void addInner(InnerSubscriber<T> inner) {
		for (; ; ) {
			InnerSubscriber<?>[] a = subscribers;
			if (a == CANCELLED) {
				Publishers.<T>empty().subscribe(inner.actual);
			}
			int n = a.length;
			InnerSubscriber<?>[] b = new InnerSubscriber[n + 1];
			System.arraycopy(a, 0, b, 0, n);
			b[n] = inner;
			if (SUBSCRIBERS.compareAndSet(this, a, b)) {
				return;
			}
		}
	}

	int getLastIndex(int n, InnerSubscriber<?>[] inner) {
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

	void removeInner(InnerSubscriber<T> inner, InnerSubscriber<?>[] lastRemoved) {
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

	void requestMore(long n) {
		int r = outstanding - (int) n;
		if (r > limit) {
			outstanding = r;
			return;
		}
		outstanding = bufferSize;
		int k = bufferSize - r;
		Subscription subscription = upstreamSubscription;
		if (subscription != SignalType.NOOP_SUBSCRIPTION && k > 0 && subscription != null) {
			subscription.request(k);
		}
	}

	public void cancel() {
		if(!done) {
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


		boolean unbounded;

		@SuppressWarnings("unused")
		private volatile long requested = -1L;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<InnerSubscriber> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(InnerSubscriber.class, "requested");

		public InnerSubscriber(EmitterProcessor<T> parent, final Subscriber<? super T> actual, long id) {
			this.id = id;
			this.actual = actual;
			this.parent = parent;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.checkRequest(n, actual)) {
				BackpressureUtils.getAndAdd(REQUESTED, this, n);
				parent.drain();
			}
		}

		public void cancel() {
			done = true;
			parent.drain();
		}

		@Override
		public boolean isExposedToOverflow(Bounded parentPublisher) {
			return parentPublisher.getCapacity() >
					parent.bufferSize;
		}

		@Override
		public long getCapacity() {
			return parent.bufferSize;
		}

		void setPollCursor(long seq) {
			Sequence pollSequence = pollCursor;
			if (pollSequence == null) {
				pollSequence = Sequencer.newSequence(seq - 1L);
				pollCursor = pollSequence;
			}
		}

		void forcePollCursor(long seq){
			Sequence pollSequence = pollCursor;
			if (pollSequence != null) {
				pollSequence.set(seq);
			}
		}

		void start() {
			if(REQUESTED.compareAndSet(this, -1L, 0)) {
				RingBuffer<RingBuffer.Slot<T>> ringBuffer = parent.emitBuffer;
				if(ringBuffer != null){
					setPollCursor(0L);
				}
				actual.onSubscribe(this);
			}
		}
	}


}
