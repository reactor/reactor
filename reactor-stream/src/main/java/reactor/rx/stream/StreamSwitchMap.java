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
package reactor.rx.stream;

import java.util.*;
import java.util.concurrent.atomic.*;
import reactor.fn.*;

import org.reactivestreams.*;

import reactor.core.error.Exceptions;
import reactor.core.subscription.*;
import reactor.core.support.*;

/**
 * Switches to a new Publisher generated via a function whenever the upstream produces an item.
 * 
 * @param <T> the source value type
 * @param <R> the output value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamSwitchMap<T, R> extends StreamBarrier<T, R> {

	final Function<? super T, ? extends Publisher<? extends R>> mapper;
	
	final Supplier<? extends Queue<Object>> queueSupplier;
	
	final int bufferSize;
	
	static final StreamSwitchMapInner<Object> CANCELLED_INNER = new StreamSwitchMapInner<>(null, 0, Long.MAX_VALUE);
	
	public StreamSwitchMap(Publisher<? extends T> source, 
			Function<? super T, ? extends Publisher<? extends R>> mapper,
					Supplier<? extends Queue<Object>> queueSupplier, int bufferSize) {
		super(source);
		if (bufferSize <= 0) {
			throw new IllegalArgumentException("BUFFER_SIZE > 0 required but it was " + bufferSize);
		}
		this.mapper = Objects.requireNonNull(mapper, "mapper");
		this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier");
		this.bufferSize = bufferSize;
	}
	
	@Override
	public void subscribe(Subscriber<? super R> s) {
		Queue<Object> q;
		
		try {
			q = queueSupplier.get();
		} catch (Throwable e) {
			EmptySubscription.error(s, e);
			return;
		}
		
		if (q == null) {
			EmptySubscription.error(s, new NullPointerException("The queueSupplier returned a null queue"));
			return;
		}
		
		source.subscribe(new StreamSwitchMapMain<>(s, mapper, q, bufferSize));
	}
	
	static final class StreamSwitchMapMain<T, R> implements Subscriber<T>, Subscription {
		
		final Subscriber<? super R> actual;
		
		final Function<? super T, ? extends Publisher<? extends R>> mapper;
		
		final Queue<Object> queue;
		
		final int bufferSize;

		Subscription s;
		
		volatile boolean done;

		volatile Throwable error;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<StreamSwitchMapMain, Throwable> ERROR =
				AtomicReferenceFieldUpdater.newUpdater(StreamSwitchMapMain.class, Throwable.class, "error");
		
		volatile boolean cancelled;
		
		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<StreamSwitchMapMain> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(StreamSwitchMapMain.class, "once");
		
		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<StreamSwitchMapMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(StreamSwitchMapMain.class, "requested");
		
		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<StreamSwitchMapMain> WIP =
				AtomicIntegerFieldUpdater.newUpdater(StreamSwitchMapMain.class, "wip");
		
		volatile StreamSwitchMapInner<R> inner;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<StreamSwitchMapMain, StreamSwitchMapInner> INNER =
				AtomicReferenceFieldUpdater.newUpdater(StreamSwitchMapMain.class, StreamSwitchMapInner.class, "inner");

		volatile long index;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<StreamSwitchMapMain> INDEX =
				AtomicLongFieldUpdater.newUpdater(StreamSwitchMapMain.class, "index");

		volatile int active;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<StreamSwitchMapMain> ACTIVE =
				AtomicIntegerFieldUpdater.newUpdater(StreamSwitchMapMain.class, "active");

		
		public StreamSwitchMapMain(Subscriber<? super R> actual,
				Function<? super T, ? extends Publisher<? extends R>> mapper, Queue<Object> queue, int bufferSize) {
			this.actual = actual;
			this.mapper = mapper;
			this.queue = queue;
			this.bufferSize = bufferSize;
			this.active = 1;
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;
				
				actual.onSubscribe(this);
				
				s.request(Long.MAX_VALUE);
			}
		}
		
		@Override
		public void onNext(T t) {
			
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}
			
			long idx = INDEX.incrementAndGet(this);
			
			StreamSwitchMapInner<R> si = inner;
			if (si != null) {
				si.deactivate();
				si.cancel();
			}
			
			Publisher<? extends R> p;
			
			try {
				p = mapper.apply(t);
			} catch (Throwable e) {
				s.cancel();
				onError(e);
				return;
			}
			
			if (p == null) {
				s.cancel();
				onError(new NullPointerException("The mapper returned a null publisher"));
				return;
			}
			
			StreamSwitchMapInner<R> innerSubscriber = new StreamSwitchMapInner<>(this, bufferSize, idx);
			
			if (INNER.compareAndSet(this, si, innerSubscriber)) {
				ACTIVE.getAndIncrement(this);
				p.subscribe(innerSubscriber);
			}
		}
		
		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}
			
			if (Exceptions.addThrowable(ERROR, this, t)) {
				
				if (ONCE.compareAndSet(this, 0, 1)) {
					deactivate();
				}
				
				cancelInner();
				done = true;
				drain();
			} else {
				Exceptions.onErrorDropped(t);
			}
		}
		
		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			
			if (ONCE.compareAndSet(this, 0, 1)) {
				deactivate();
			}

			done = true;
			drain();
		}
		
		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.addAndGet(REQUESTED, this, n);
				drain();
			}
		}
		
		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				
				if (WIP.getAndIncrement(this) == 0) {
					cancelAndCleanup(queue);
				}
			}
		}
		
		void deactivate() {
			ACTIVE.decrementAndGet(this);
		}

		void cancelInner() {
			StreamSwitchMapInner<?> si = INNER.getAndSet(this, CANCELLED_INNER);
			if (si != null && si != CANCELLED_INNER) {
				si.cancel();
				si.deactivate();
			}
		}
		
		void cancelAndCleanup(Queue<?> q) {
			s.cancel();
			
			cancelInner();
			
			q.clear();
		}
		
		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}
			
			Subscriber<? super R> a = actual;
			Queue<Object> q = queue;
			
			int missed = 1;
			
			for (;;) {
				
				long r = requested;
				long e = 0L;
				
				while (r != e) {
					boolean d = active == 0;
					
					@SuppressWarnings("unchecked")
					StreamSwitchMapInner<R> si = (StreamSwitchMapInner<R>)q.poll();
					
					boolean empty = si == null;
					
					if (checkTerminated(d, empty, a, q)) {
						return;
					}
					
					if (empty) {
						break;
					}
					
					Object second;
					
					while ((second = q.poll()) == null) ;
					
					if (index == si.index) {
						
						@SuppressWarnings("unchecked")
						R v = (R)second;
						
						a.onNext(v);
						
						si.requestOne();
						
						e++;
					}
				}
				
				if (r == e) {
					if (checkTerminated(active == 0, q.isEmpty(), a, q)) {
						return;
					}
				}
				
				if (e != 0 && r != Long.MAX_VALUE) {
					REQUESTED.addAndGet(this, -e);
				}
				
				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}
		
		boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a, Queue<?> q) {
			if (cancelled) {
				cancelAndCleanup(q);
				return true;
			}
			
			if (d) {
				Throwable e = Exceptions.terminate(ERROR, this);
				if (e != null && e != Exceptions.TERMINATED) {
					cancelAndCleanup(q);
					
					a.onError(e);
					return true;
				} else
				if (empty) {
					a.onComplete();
					return true;
				}
				
			}
			return false;
		}
		
		void innerNext(StreamSwitchMapInner<R> inner, R value) {
			queue.offer(inner);
			queue.offer(value);
			drain();
		}
		
		void innerError(StreamSwitchMapInner<R> inner, Throwable e) {
			if (Exceptions.addThrowable(ERROR, this, e)) {
				s.cancel();
				
				if (ONCE.compareAndSet(this, 0, 1)) {
					deactivate();
				}
				inner.deactivate();
				drain();
			} else {
				Exceptions.onErrorDropped(e);
			}
		}
		
		void innerComplete(StreamSwitchMapInner<R> inner) {
			inner.deactivate();
			drain();
		}
	}
	
	static final class StreamSwitchMapInner<R> implements Subscriber<R>, Subscription {
		
		final StreamSwitchMapMain<?, R> parent;
		
		final int bufferSize;
		
		final int limit;
		
		final long index;
		
		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<StreamSwitchMapInner> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(StreamSwitchMapInner.class, "once");
		
		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<StreamSwitchMapInner, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(StreamSwitchMapInner.class, Subscription.class, "s");
		
		int produced;
		
		public StreamSwitchMapInner(StreamSwitchMapMain<?, R> parent, int bufferSize, long index) {
			this.parent = parent;
			this.bufferSize = bufferSize;
			this.limit = bufferSize - (bufferSize >> 2);
			this.index = index;
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			Subscription a = this.s;
			if (a == CancelledSubscription.INSTANCE) {
				s.cancel();
			}
			if (a != null) {
				s.cancel();
				
				BackpressureUtils.reportSubscriptionSet();
				return;
			}
			
			if (S.compareAndSet(this, null, s)) {
				s.request(bufferSize);
				return;
			}
			a = this.s;
			if (a != CancelledSubscription.INSTANCE) {
				s.cancel();
				
				BackpressureUtils.reportSubscriptionSet();
				return;
			}
		}
		
		@Override
		public void onNext(R t) {
			parent.innerNext(this, t);
		}
		
		@Override
		public void onError(Throwable t) {
			parent.innerError(this, t);
		}
		
		@Override
		public void onComplete() {
			parent.innerComplete(this);
		}
		
		void deactivate() {
			if (ONCE.compareAndSet(this, 0, 1)) {
				parent.deactivate();
			}
		}
		
		void requestOne() {
			int p = produced + 1;
			if (p == limit) {
				produced = 0;
				s.request(p);
			} else {
				produced = p;
			}
		}
		
		@Override
		public void request(long n) {
			long p = produced + n;
			if (p >= limit) {
				produced = 0;
				s.request(p);
			} else {
				produced = (int)p;
			}
		}
		
		@Override
		public void cancel() {
			Subscription a = s;
			if (a != CancelledSubscription.INSTANCE) {
				a = S.getAndSet(this, CancelledSubscription.INSTANCE);
				if (a != null && a != CancelledSubscription.INSTANCE) {
					a.cancel();
				}
			}
		}
	}
}
