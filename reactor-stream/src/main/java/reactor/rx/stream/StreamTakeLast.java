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

import java.util.ArrayDeque;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import reactor.fn.BooleanSupplier;

import org.reactivestreams.*;

import reactor.core.subscriber.SubscriberDeferredScalar;
import reactor.core.support.*;

/**
 * Emits the last N values the source emitted before its completion.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamTakeLast<T> extends StreamBarrier<T, T> {

	final int n;

	public StreamTakeLast(Publisher<? extends T> source, int n) {
		super(source);
		if (n < 0) {
			throw new IllegalArgumentException("n >= required but it was " + n);
		}
		this.n = n;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (n == 0) {
			source.subscribe(new StreamTakeLastZeroSubscriber<>(s));
		} else if (n == 1) {
			source.subscribe(new StreamTakeLastOneSubscriber<>(s));
		} else {
			source.subscribe(new StreamTakeLastManySubscriber<>(s, n));
		}
	}

	static final class StreamTakeLastZeroSubscriber<T> implements Subscriber<T>,
																	 Downstream {

		final Subscriber<? super T> actual;

		public StreamTakeLastZeroSubscriber(Subscriber<? super T> actual) {
			this.actual = actual;
		}

		@Override
		public void onSubscribe(Subscription s) {
			actual.onSubscribe(s);

			s.request(Long.MAX_VALUE);
		}

		@Override
		public void onNext(T t) {
			// ignoring all values
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {
			actual.onComplete();
		}

		@Override
		public Object downstream() {
			return actual;
		}
	}

	static final class StreamTakeLastOneSubscriber<T>
			extends SubscriberDeferredScalar<T, T>
	implements Upstream {

		Subscription s;

		public StreamTakeLastOneSubscriber(Subscriber<? super T> actual) {
			super(actual);
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;

				subscriber.onSubscribe(this);

				s.request(Long.MAX_VALUE);
			}

		}

		@Override
		public void onNext(T t) {
			value = t;
		}

		@Override
		public void onComplete() {
			T v = value;
			if (v == null) {
				subscriber.onComplete();
			}
			set(v);
		}

		@Override
		public void cancel() {
			super.cancel();
			s.cancel();
		}

		@Override
		public void setValue(T value) {
			// value is always in a field
		}

		@Override
		public Object upstream() {
			return s;
		}
	}

	static final class StreamTakeLastManySubscriber<T>
	  implements Subscriber<T>, Subscription, BooleanSupplier, Downstream, ActiveDownstream, Upstream, Buffering {

		final Subscriber<? super T> actual;

		final int n;

		volatile boolean cancelled;

		Subscription s;

		final ArrayDeque<T> buffer;

		volatile long requested;
		@SuppressWarnings("rawtypes")
		static final AtomicLongFieldUpdater<StreamTakeLastManySubscriber> REQUESTED =
		  AtomicLongFieldUpdater.newUpdater(StreamTakeLastManySubscriber.class, "requested");

		public StreamTakeLastManySubscriber(Subscriber<? super T> actual, int n) {
			this.actual = actual;
			this.n = n;
			this.buffer = new ArrayDeque<>();
		}

		@Override
		public boolean getAsBoolean() {
			return cancelled;
		}

		@Override
		public void request(long n) {
			if (BackpressureUtils.validate(n)) {
				BackpressureUtils.postCompleteRequest(n, actual, buffer, REQUESTED, this, this);
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

				s.request(Long.MAX_VALUE);
			}
		}

		@Override
		public void onNext(T t) {
			ArrayDeque<T> bs = buffer;

			if (bs.size() == n) {
				bs.poll();
			}
			bs.offer(t);
		}

		@Override
		public void onError(Throwable t) {
			actual.onError(t);
		}

		@Override
		public void onComplete() {

			BackpressureUtils.postComplete(actual, buffer, REQUESTED, this, this);
		}

		@Override
		public Object upstream() {
			return s;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public long pending() {
			return buffer.size();
		}

		@Override
		public long getCapacity() {
			return n;
		}

		@Override
		public Object downstream() {
			return actual;
		}
	}
}
