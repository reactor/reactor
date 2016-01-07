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

import java.util.concurrent.TimeUnit;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.timer.Timer;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.5
 */
public final class StreamDebounce<T> extends StreamBatch<T, T> {

	public StreamDebounce(Publisher<T> source, int maxSize) {
		this(source, maxSize, false);
	}

	public StreamDebounce(Publisher<T> source, int maxSize, boolean first) {
		super(source, maxSize, !first, first, true);
	}

	public StreamDebounce(Publisher<T> source, boolean first, int maxSize, long timespan, TimeUnit unit, Timer timer) {
		super(source, maxSize, !first, first, true, timespan, unit, timer);
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new SampleAction<T>(prepareSub(subscriber), first, batchSize, timespan, unit, timer);
	}

	final static class SampleAction<T> extends StreamBatch.BatchAction<T, T> {

		private T sample;

		public SampleAction(Subscriber<? super T> actual,
				boolean first,
				int maxSize,
				long timespan,
				TimeUnit unit,
				Timer timer) {

			super(actual, maxSize, !first, first, true, timespan, unit, timer);
		}

		@Override
		protected void firstCallback(T event) {
			sample = event;
		}

		@Override
		protected void nextCallback(T event) {
			sample = event;
		}

		@Override
		protected void flushCallback(T event) {
			if (sample != null) {
				T _last = sample;
				sample = null;
				subscriber.onNext(_last);
			}
		}

		@Override
		public String toString() {
			return super.toString() + " every=" + sample;
		}
	}
}
