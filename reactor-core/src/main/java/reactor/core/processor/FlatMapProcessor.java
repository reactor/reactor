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

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.error.SpecificationExceptions;
import reactor.core.processor.rb.MutableSignal;
import reactor.core.processor.rb.RingBufferSubscriberUtils;
import reactor.core.processor.rb.disruptor.BusySpinWaitStrategy;
import reactor.core.processor.rb.disruptor.RingBuffer;
import reactor.core.processor.rb.disruptor.WaitStrategy;
import reactor.core.subscriber.BaseSubscriber;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.Bounded;
import reactor.core.support.SignalType;
import reactor.fn.Function;
import reactor.fn.Supplier;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A merge processor that transforms input T to Publisher of V using the assigned map function.
 * Once transformed, publishers are then subscribed and requested with the following demand rules:
 * - request eagerly up to bufferSize
 * - subscriber consume buffer once all subscribers have read
 * - when more than half of the demand has been fulfilled, request more
 * - request to upstream represent the maximum concurrent merge possible
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public final class FlatMapProcessor<T, V> extends BaseSubscriber<T> implements Processor<T, V>, Bounded, Subscription {

	private final Function<? super T, ? extends Publisher<? extends V>> map;
	private final int                                                   maxConcurrency;

	private final MergeState<T, V> state;

	public FlatMapProcessor(Function<? super T, ? extends Publisher<? extends V>> map, int bufferSize, int maxConcurrency) {
		this.map = map;

		this.maxConcurrency = maxConcurrency;
		this.state = new MergeState<>(maxConcurrency, bufferSize);

	}

	@Override
	public void subscribe(Subscriber<? super V> s) {

	}

	@Override
	public void onSubscribe(Subscription s) {
		super.onSubscribe(s);
	}

	@Override
	public void onNext(T t) {
		super.onNext(t);
		try {
			Publisher<? extends V> publisher = map.apply(t);
			InnerSubscriber<T, V> inner = new InnerSubscriber<>(this);
			state.add(inner);
			publisher.subscribe(inner);
		} catch (Throwable e) {
			onError(e);
		}
	}

	@Override
	public void onError(Throwable t) {
		super.onError(t);
		state.drain();
	}

	@Override
	public void onComplete() {
		state.drain();
	}

	@Override
	public void request(long n) {
		try {
			BackpressureUtils.checkRequest(n);
		} catch (SpecificationExceptions.Spec309_NullOrNegativeRequest iae){
			//subscriber.onError(iae);
			return;
		}
	}

	@Override
	public void cancel() {
		if(state.tryRunning()){

		}
	}

	@Override
	public boolean isExposedToOverflow(Bounded parentPublisher) {
		return false;
	}

	public int getMaxConcurrency() {
		return maxConcurrency;
	}

	@Override
	public long getCapacity() {
		return state.emitBuffer.getBufferSize();
	}

	static class MergeState<T, V> {

		final RingBuffer<Emitted<T, V>>                             emitBuffer;
		
		volatile     int                                   running = 0;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<MergeState> RUNNING =
		  AtomicIntegerFieldUpdater.newUpdater(MergeState.class, "running");

		volatile InnerSubscriber<?, ?>[] subscribers;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MergeState, InnerSubscriber[]> SUBSCRIBERS =
		  AtomicReferenceFieldUpdater.newUpdater(MergeState.class, InnerSubscriber[].class, "subscribers");

		static final InnerSubscriber<?, ?>[] EMPTY     = new InnerSubscriber<?, ?>[0];
		static final InnerSubscriber<?, ?>[] CANCELLED = new InnerSubscriber<?, ?>[0];

		public MergeState(int maxConcurrency, int bufferSize) {

			WaitStrategy waitStrategy = new BusySpinWaitStrategy();
			Supplier<Emitted<T, V>> factory = new Supplier<Emitted<T, V>>() {
				@Override
				public Emitted<T, V> get() {
					return new Emitted<T, V>();
				}
			};

			if (maxConcurrency == 1) {
				emitBuffer = RingBuffer.createSingleProducer(factory, bufferSize, waitStrategy);
			} else {
				emitBuffer = RingBuffer.createMultiProducer(factory, bufferSize, waitStrategy);

			}
			
			SUBSCRIBERS.lazySet(this, EMPTY);
			
		}

		void add(InnerSubscriber<T, V> inner) {
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

		void remove(InnerSubscriber<T, V> inner) {

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

		boolean tryRunning(){
			return RUNNING.getAndIncrement(this) == 0;
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

		void drain(){
			if(tryRunning()){
				drainLoop();
			}
		}

		void drainLoop(){

		}
	}

	private static class Emitted<T, V> {
		final MutableSignal<V> signal = new MutableSignal<>();
		InnerSubscriber<T, V> inner;
	}

	private static class InnerSubscriber<T, V> extends BaseSubscriber<V> implements Bounded, Subscription {

		final FlatMapProcessor<T, V> processor;
		final RingBuffer<Emitted<T, V>> emitBuffer;

		public InnerSubscriber(FlatMapProcessor<T, V> processor) {
			this.processor = processor;
			this.emitBuffer = processor.state.emitBuffer;
		}

		@Override
		public void onSubscribe(Subscription s) {
			super.onSubscribe(s);
		}

		@Override
		public void onNext(V t) {
			super.onNext(t);

			final long seqId = emitBuffer.next();
			final Emitted<T, V> emitted = emitBuffer.get(seqId);
			emitted.signal.type = SignalType.NEXT;
			emitted.signal.value = t;
			emitted.inner = this;

			emitBuffer.publish(seqId);
		}

		@Override
		public void onError(Throwable t) {
			final long seqId = emitBuffer.next();
			final Emitted<T, V> emitted = emitBuffer.get(seqId);
			emitted.signal.type = SignalType.ERROR;
			emitted.signal.error = t;
			emitted.signal.value = null;
			emitted.inner = this;

			emitBuffer.publish(seqId);
		}

		@Override
		public void onComplete() {
			final long seqId = emitBuffer.next();
			final Emitted<T, V> emitted = emitBuffer.get(seqId);
			emitted.signal.type = SignalType.COMPLETE;
			emitted.signal.value = null;
			emitted.inner = this;

			emitBuffer.publish(seqId);
		}

		@Override
		public boolean isExposedToOverflow(Bounded parentPublisher) {
			return false;
		}

		@Override
		public long getCapacity() {
			return 0;
		}

		@Override
		public void request(long n) {

		}

		@Override
		public void cancel() {

		}
	}
}
