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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.*;

import reactor.core.subscriber.SubscriberMultiSubscription;
import reactor.core.subscription.EmptySubscription;
import reactor.core.support.ReactiveState;

/**
 * Concatenates a fixed array of Publishers' values.
 *
 * @param <T> the value type
 */

/**
 * {@see <a href='https://github.com/reactor/reactive-streams-commons'>https://github.com/reactor/reactive-streams-commons</a>}
 * @since 2.5
 */
public final class StreamConcatArray<T> 
extends reactor.rx.Stream<T>
implements 
													  ReactiveState.Factory,
													  ReactiveState.LinkedUpstreams {

	final Publisher<? extends T>[] array;

	@SafeVarargs
	@SuppressWarnings("varargs")
	public StreamConcatArray(Publisher<? extends T>... array) {
		this.array = Objects.requireNonNull(array, "array");
	}

	@Override
	public Iterator<?> upstreams() {
		return Arrays.asList(array).iterator();
	}

	@Override
	public long upstreamsCount() {
		return array.length;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		Publisher<? extends T>[] a = array;

		if (a.length == 0) {
			EmptySubscription.complete(s);
			return;
		}
		if (a.length == 1) {
			Publisher<? extends T> p = a[0];

			if (p == null) {
				EmptySubscription.error(s, new NullPointerException("The single source Publisher is null"));
			} else {
				p.subscribe(s);
			}
			return;
		}

		StreamConcatArraySubscriber<T> parent = new StreamConcatArraySubscriber<>(s, a);

		s.onSubscribe(parent);

		if (!parent.isCancelled()) {
			parent.onComplete();
		}
	}

	static final class StreamConcatArraySubscriber<T>
	  extends SubscriberMultiSubscription<T, T> {

		final Publisher<? extends T>[] sources;

		int index;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<StreamConcatArraySubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(StreamConcatArraySubscriber.class, "wip");

		long produced;

		public StreamConcatArraySubscriber(Subscriber<? super T> actual, Publisher<? extends T>[] sources) {
			super(actual);
			this.sources = sources;
		}

		@Override
		public void onNext(T t) {
			produced++;

			subscriber.onNext(t);
		}

		@Override
		public void onComplete() {
			if (WIP.getAndIncrement(this) == 0) {
				Publisher<? extends T>[] a = sources;
				do {

					if (isCancelled()) {
						return;
					}

					int i = index;
					if (i == a.length) {
						subscriber.onComplete();
						return;
					}

					Publisher<? extends T> p = a[i];

					if (p == null) {
						subscriber.onError(new NullPointerException("The " + i + "th source Publisher is null"));
						return;
					}

					long c = produced;
					if (c != 0L) {
						produced = 0L;
						produced(c);
					}
					p.subscribe(this);

					if (isCancelled()) {
						return;
					}

					index = ++i;
				} while (WIP.decrementAndGet(this) != 0);
			}

		}
	}
}
