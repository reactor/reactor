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
import reactor.fn.Supplier;

import org.reactivestreams.*;

import reactor.core.error.Exceptions;
import reactor.core.subscription.DeferredSubscription;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.*;

/**
 * Buffers elements into custom collections where the buffer boundary is signalled
 * by another publisher.
 *
 * @param <T> the source value type
 * @param <U> the element type of the boundary publisher (irrelevant)
 * @param <C> the output collection type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamBufferBoundary<T, U, C extends Collection<? super T>> 
extends StreamBarrier<T, C> {

	final Publisher<U> other;
	
	final Supplier<C> bufferSupplier;

	public StreamBufferBoundary(Publisher<? extends T> source, 
			Publisher<U> other, Supplier<C> bufferSupplier) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
		this.bufferSupplier = Objects.requireNonNull(bufferSupplier, "bufferSupplier");
	}
	
	@Override
	public void subscribe(Subscriber<? super C> s) {
		C buffer;
		
		try {
			buffer = bufferSupplier.get();
		} catch (Throwable e) {
			EmptySubscription.error(s, e);
			return;
		}
		
		if (buffer == null) {
			EmptySubscription.error(s, new NullPointerException("The bufferSupplier returned a null buffer"));
			return;
		}
		
		StreamBufferBoundaryMain<T, U, C> parent = new StreamBufferBoundaryMain<>(s, buffer, bufferSupplier);
		
		StreamBufferBoundaryOther<U> boundary = new StreamBufferBoundaryOther<>(parent);
		parent.other = boundary;
		
		s.onSubscribe(parent);
		
		other.subscribe(boundary);
		
		source.subscribe(parent);
	}
	
	static final class StreamBufferBoundaryMain<T, U, C extends Collection<? super T>>
	implements Subscriber<T>, Subscription {

		final Subscriber<? super C> actual;
		
		final Supplier<C> bufferSupplier;
		
		StreamBufferBoundaryOther<U> other;
		
		C buffer;
		
		volatile Subscription s;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<StreamBufferBoundaryMain, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(StreamBufferBoundaryMain.class, Subscription.class, "s");
		
		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<StreamBufferBoundaryMain> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(StreamBufferBoundaryMain.class, "requested");
		
		public StreamBufferBoundaryMain(Subscriber<? super C> actual, C buffer, Supplier<C> bufferSupplier) {
			this.actual = actual;
			this.buffer = buffer;
			this.bufferSupplier = bufferSupplier;
		}
		
		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.addAndGet(REQUESTED, this, n);
			}
		}

		void cancelMain() {
			BackpressureUtils.terminate(S, this);
		}
		
		@Override
		public void cancel() {
			cancelMain();
			other.cancel();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.setOnce(S, this, s)) {
				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			synchronized (this) {
				C b = buffer;
				if (b != null) {
					b.add(t);
					return;
				}
			}
			
			Exceptions.onNextDropped(t);
		}

		@Override
		public void onError(Throwable t) {
			boolean report;
			synchronized (this) {
				C b = buffer;
				
				if (b != null) {
					buffer = null;
					report = true;
				} else {
					report = false;
				}
			}
			
			if (report) {
				other.cancel();
				
				actual.onError(t);
			} else {
				Exceptions.onErrorDropped(t);
			}
		}

		@Override
		public void onComplete() {
			C b;
			synchronized (this) {
				b = buffer;
				buffer = null;
			}
			
			if (b != null) {
				if (emit(b)) {
					actual.onComplete();
				}
			}
		}
		
		void otherNext() {
			C c;
			
			try {
				c = bufferSupplier.get();
			} catch (Throwable e) {
				other.cancel();
				
				otherError(e);
				return;
			}
			
			if (c == null) {
				other.cancel();

				otherError(new NullPointerException("The bufferSupplier returned a null buffer"));
				return;
			}
			
			C b;
			synchronized (this) {
				b = buffer;
				if (b == null) {
					return;
				}
				buffer = c;
			}
			
			emit(b);
		}
		
		void otherError(Throwable e) {
			cancelMain();
			
			onError(e);
		}
		
		void otherComplete() {
			// FIXME let the last buffer fill until the main completes?
		}
		
		boolean emit(C b) {
			long r = requested;
			if (r != 0L) {
				actual.onNext(b);
				if (r != Long.MAX_VALUE) {
					REQUESTED.decrementAndGet(this);
				}
				return true;
			} else {
				cancel();
				
				actual.onError(new IllegalStateException("Could not emit buffer due to lack of requests"));

				return false;
			}
		}
	}
	
	static final class StreamBufferBoundaryOther<U> extends DeferredSubscription
	implements Subscriber<U> {
		
		final StreamBufferBoundaryMain<?, U, ?> main;
		
		public StreamBufferBoundaryOther(StreamBufferBoundaryMain<?, U, ?> main) {
			this.main = main;
		}
		
		@Override
		public void onSubscribe(Subscription s) {
			if (set(s)) {
				s.request(Long.MAX_VALUE);
			}
		}
		
		@Override
		public void onNext(U t) {
			main.otherNext();
		}
		
		@Override
		public void onError(Throwable t) {
			main.otherError(t);
		}
		
		@Override
		public void onComplete() {
			main.otherComplete();
		}
	}
}
