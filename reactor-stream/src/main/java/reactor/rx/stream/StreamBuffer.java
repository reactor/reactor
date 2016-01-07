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
import reactor.core.support.*;

/**
 * Buffers a certain number of subsequent elements and emits the buffers.
 *
 * @param <T> the source value type
 * @param <C> the buffer collection type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamBuffer<T, C extends Collection<? super T>> extends StreamBarrier<T, C>
		implements ReactiveState.Bounded {

	final int size;

	final int skip;

	final Supplier<C> bufferSupplier;

	public StreamBuffer(Publisher<? extends T> source, int size, Supplier<C> bufferSupplier) {
		this(source, size, size, bufferSupplier);
	}

	public StreamBuffer(Publisher<? extends T> source, int size, int skip, Supplier<C> bufferSupplier) {
		super(source);
		if (size <= 0) {
			throw new IllegalArgumentException("size > 0 required but it was " + size);
		}

		if (skip <= 0) {
			throw new IllegalArgumentException("skip > 0 required but it was " + size);
		}

		this.size = size;
		this.skip = skip;
		this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
	}

	@Override
	public void subscribe(Subscriber<? super C> s) {
		if (size == skip) {
			source.subscribe(new StreamBufferExactSubscriber<>(s, size, bufferSupplier));
		} else if (skip > size) {
			source.subscribe(new StreamBufferSkipSubscriber<>(s, size, skip, bufferSupplier));
		} else {
			source.subscribe(new StreamBufferOverlappingSubscriber<>(s, size, skip, bufferSupplier));
		}
	}

	@Override
	public long getCapacity() {
		return size;
	}

	static final class StreamBufferExactSubscriber<T, C extends Collection<? super T>>
	  implements Subscriber<T>, Subscription, Downstream, FeedbackLoop, Upstream, ActiveUpstream, Buffering {

		final Subscriber<? super C> actual;

		final Supplier<C> bufferSupplier;

		final int size;

		C buffer;

		Subscription s;

		boolean done;

		public StreamBufferExactSubscriber(Subscriber<? super C> actual, int size, Supplier<C> bufferSupplier) {
			this.actual = actual;
			this.size = size;
			this.bufferSupplier = bufferSupplier;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				s.request(BackpressureUtils.multiplyCap(n, size));
			}
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}

			C b = buffer;
			if (b == null) {

				try {
					b = bufferSupplier.get();
				} catch (Throwable e) {
					cancel();

					onError(e);
					return;
				}

				if (b == null) {
					cancel();

					onError(new NullPointerException("The bufferSupplier returned a null buffer"));
					return;
				}
				buffer = b;
			}

			b.add(t);

			if (b.size() == size) {
				buffer = null;
				actual.onNext(b);
			}
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}
			done = true;
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}
			done = true;

			C b = buffer;

			if (b != null && !b.isEmpty()) {
				actual.onNext(b);
			}
			actual.onComplete();
		}

		@Override
		public boolean isStarted() {
			return s != null && !done;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object delegateInput() {
			return bufferSupplier;
		}

		@Override
		public Object delegateOutput() {
			return buffer;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public long pending() {
			C b = buffer;
			return b != null ? b.size() : 0L;
		}

		@Override
		public long getCapacity() {
			return size;
		}
	}

	static final class StreamBufferSkipSubscriber<T, C extends Collection<? super T>>
	  implements Subscriber<T>, Subscription, Downstream, FeedbackLoop, Upstream, ActiveUpstream, Buffering {

		final Subscriber<? super C> actual;

		final Supplier<C> bufferSupplier;

		final int size;

		final int skip;

		C buffer;

		Subscription s;

		boolean done;

		long index;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<StreamBufferSkipSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(StreamBufferSkipSubscriber.class, "wip");

		public StreamBufferSkipSubscriber(Subscriber<? super C> actual, int size, int skip,
											 Supplier<C> bufferSupplier) {
			this.actual = actual;
			this.size = size;
			this.skip = skip;
			this.bufferSupplier = bufferSupplier;
		}

		@Override
		public void request(long n) {
			if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
				// n full buffers
				long u = BackpressureUtils.multiplyCap(n, size);
				// + (n - 1) gaps
				long v = BackpressureUtils.multiplyCap(skip - size, n - 1);

				s.request(BackpressureUtils.addCap(u, v));
			} else {
				// n full buffer + gap
				s.request(BackpressureUtils.multiplyCap(skip, n));
			}
		}

		@Override
		public void cancel() {
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}

			C b = buffer;

			long i = index;

			if (i % skip == 0L) {
				try {
					b = bufferSupplier.get();
				} catch (Throwable e) {
					cancel();

					onError(e);
					return;
				}

				if (b == null) {
					cancel();

					onError(new NullPointerException("The bufferSupplier returned a null buffer"));
					return;
				}

				buffer = b;
			}

			if (b != null) {
				b.add(t);
				if (b.size() == size) {
					buffer = null;
					actual.onNext(b);
				}
			}

			index = i + 1;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}

			done = true;
			buffer = null;

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}

			done = true;
			C b = buffer;
			buffer = null;

			if (b != null) {
				actual.onNext(b);
			}

			actual.onComplete();
		}

		@Override
		public boolean isStarted() {
			return s != null && !done;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public Object delegateInput() {
			return bufferSupplier;
		}

		@Override
		public Object delegateOutput() {
			return buffer;
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public long pending() {
			C b = buffer;
			return b != null ? b.size() : 0L;
		}

		@Override
		public long getCapacity() {
			return size;
		}
	}


	static final class StreamBufferOverlappingSubscriber<T, C extends Collection<? super T>>
	  implements Subscriber<T>, Subscription, BooleanSupplier, Downstream, Upstream, ActiveUpstream,
				 ActiveDownstream, FeedbackLoop, Buffering, DownstreamDemand {
		final Subscriber<? super C> actual;

		final Supplier<C> bufferSupplier;

		final int size;

		final int skip;

		final ArrayDeque<C> buffers;

		Subscription s;

		boolean done;

		long index;

		volatile boolean cancelled;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<StreamBufferOverlappingSubscriber> ONCE =
		  AtomicIntegerFieldUpdater.newUpdater(StreamBufferOverlappingSubscriber.class, "once");

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<StreamBufferOverlappingSubscriber> REQUESTED =
		  AtomicLongFieldUpdater.newUpdater(StreamBufferOverlappingSubscriber.class, "requested");

		public StreamBufferOverlappingSubscriber(Subscriber<? super C> actual, int size, int skip,
													Supplier<C> bufferSupplier) {
			this.actual = actual;
			this.size = size;
			this.skip = skip;
			this.bufferSupplier = bufferSupplier;
			this.buffers = new ArrayDeque<>();
		}

		@Override
		public boolean getAsBoolean() {
			return cancelled;
		}

		@Override
		public void request(long n) {

			if (!BackpressureUtils.validate(n)) {
				return;
			}

			if (BackpressureUtils.postCompleteRequest(n, actual, buffers, REQUESTED, this, this)) {
				return;
			}

			if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
				// (n - 1) skips
				long u = BackpressureUtils.multiplyCap(skip, n - 1);

				// + 1 full buffer
				long r = BackpressureUtils.addCap(size, u);
				s.request(r);
			} else {
				// n skips
				long r = BackpressureUtils.multiplyCap(skip, n);
				s.request(r);
			}
		}

		@Override
		public void cancel() {
			cancelled = true;
			s.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			if (done) {
				Exceptions.onNextDropped(t);
				return;
			}

			ArrayDeque<C> bs = buffers;

			long i = index;

			if (i % skip == 0L) {
				C b;

				try {
					b = bufferSupplier.get();
				} catch (Throwable e) {
					cancel();

					onError(e);
					return;
				}

				if (b == null) {
					cancel();

					onError(new NullPointerException("The bufferSupplier returned a null buffer"));
					return;
				}

				bs.offer(b);
			}

			C b = bs.peek();

			if (b != null && b.size() + 1 == size) {
				bs.poll();

				b.add(t);

				actual.onNext(b);

				if (requested != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}
			}

			for (C b0 : bs) {
				b0.add(t);
			}

			index = i + 1;
		}

		@Override
		public void onError(Throwable t) {
			if (done) {
				Exceptions.onErrorDropped(t);
				return;
			}

			done = true;
			buffers.clear();

			actual.onError(t);
		}

		@Override
		public void onComplete() {
			if (done) {
				return;
			}

			done = true;

			BackpressureUtils.postComplete(actual, buffers, REQUESTED, this, this);
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return s != null && (!cancelled && !done);
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public long pending() {
			return buffers.size()*size; //rounded max
		}

		@Override
		public long getCapacity() {
			return Long.MAX_VALUE;
		}

		@Override
		public Object downstream() {
			return actual;
		}

		@Override
		public long requestedFromDownstream() {
			return requested;
		}

		@Override
		public Object delegateInput() {
			return bufferSupplier;
		}

		@Override
		public Object delegateOutput() {
			return buffers;
		}

		@Override
		public Object upstream() {
			return s;
		}
	}
}
