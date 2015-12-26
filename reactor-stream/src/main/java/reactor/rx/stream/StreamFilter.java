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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Predicate;

/**
 * @author Stephane Maldini
 * @since 1.1, 2.1
 */
public final class StreamFilter<T> extends StreamBarrier<T, T> {

	public static final Predicate<Boolean> simplePredicate = new Predicate<Boolean>() {
		@Override
		public boolean test(Boolean aBoolean) {
			return aBoolean;
		}
	};

	private final Predicate<? super T> p;

	public StreamFilter(Publisher<T> source, Predicate<? super T> p) {
		super(source);
		this.p = p;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new FilterAction<>(subscriber, p);
	}

	static final class FilterAction<T> extends SubscriberBarrier<T, T> {

		private final Predicate<? super T> p;

		public FilterAction(Subscriber<? super T> actual, Predicate<? super T> p) {
			super(actual);
			this.p = p;
		}

		@Override
		protected void doNext(T value) {
			if (p.test(value)) {
				subscriber.onNext(value);
			}
			else {
				doRequest(1);
				// GH-154: Verbose error level logging of every event filtered out by a Stream filter
				// Fix: ignore Predicate failures and drop values rather than notifying of errors.
				//d.accept(new IllegalArgumentException(String.format("%s failed a predicate test.", value)));
			}
		}
	}
}