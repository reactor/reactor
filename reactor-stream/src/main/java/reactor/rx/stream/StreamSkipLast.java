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

import org.reactivestreams.*;

import reactor.core.support.BackpressureUtils;

/**
 * Skips the last N elements from the source stream.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamSkipLast<T> extends StreamBarrier<T, T> {

	final int n;

	public StreamSkipLast(Publisher<? extends T> source, int n) {
		super(source);
		if (n < 0) {
			throw new IllegalArgumentException("n >= 0 required but it was " + n);
		}
		this.n = n;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if (n == 0) {
			source.subscribe(s);
		} else {
			source.subscribe(new StreamSkipLastSubscriber<>(s, n));
		}
	}

	static final class StreamSkipLastSubscriber<T> implements Subscriber<T>, Upstream, Downstream,
																 Buffering {
		final Subscriber<? super T> actual;

		final int n;

		final ArrayDeque<T> buffer;

		Subscription s;

		public StreamSkipLastSubscriber(Subscriber<? super T> actual, int n) {
			this.actual = actual;
			this.n = n;
			this.buffer = new ArrayDeque<>();
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (BackpressureUtils.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(s);

				s.request(n);
			}
		}

		@Override
		public void onNext(T t) {

			ArrayDeque<T> bs = buffer;

			if (bs.size() == n) {
				T v = bs.poll();

				actual.onNext(v);
			}
			bs.offer(t);

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

		@Override
		public Object upstream() {
			return s;
		}
	}
}
