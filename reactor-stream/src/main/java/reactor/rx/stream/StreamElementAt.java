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

/**
 * @author Anatoly Kadyshev
 * @since 2.0, 2.1
 */
public final class StreamElementAt<T> extends StreamBarrier<T, T> {

	private final int     index;
	private final T       defaultValue;
	private final boolean defaultProvided;

	public StreamElementAt(Publisher<T> source, int index) {
		this(source, index, null, false);
	}

	public StreamElementAt(Publisher<T> source, int index, T defaultValue) {
		this(source, index, defaultValue, true);
	}

	public StreamElementAt(Publisher<T> source, int index, T defaultValue, boolean defaultProvided) {
		super(source);
		if (index < 0) {
			throw new IndexOutOfBoundsException("index should be >= 0");
		}
		this.index = index;
		this.defaultValue = defaultValue;
		this.defaultProvided = defaultProvided;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new ElementAtAction<T>(subscriber, index, defaultValue, defaultProvided);
	}

	static final class ElementAtAction<T> extends SubscriberBarrier<T, T> {

		private final int     index;
		private final T       defaultValue;
		private final boolean defaultProvided;

		private int currentIndex = 0;

		public ElementAtAction(Subscriber<? super T> actual, int index, T defaultValue, boolean defaultProvided) {
			super(actual);
			this.defaultValue = defaultValue;
			this.defaultProvided = defaultProvided;
			this.index = index;
		}

		@Override
		protected void doNext(T ev) {
			if (currentIndex == index) {
				cancel();
				subscriber.onNext(ev);
				subscriber.onComplete();
			}
			currentIndex++;
		}

		@Override
		public void doComplete() {
			if (currentIndex <= index) {
				if (defaultProvided) {
					subscriber.onNext(defaultValue);
				}
				else {
					subscriber.onError(new IndexOutOfBoundsException("index is out of bounds"));
					return;
				}
			}
			super.doComplete();
		}
	}
}
