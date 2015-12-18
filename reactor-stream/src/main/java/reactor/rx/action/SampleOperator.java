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

package reactor.rx.action;

import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscriber;
import reactor.core.timer.Timer;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class SampleOperator<T> extends BatchOperator<T, T> {

	public SampleOperator(int maxSize) {
		this(maxSize, false);
	}

	public SampleOperator(int maxSize, boolean first) {
		super(maxSize, !first, first, true);
	}

	public SampleOperator(boolean first, int maxSize, long timespan, TimeUnit unit, Timer timer) {
		super(maxSize, !first, first, true, timespan, unit, timer);
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new SampleAction<T>(prepareSub(subscriber), first, batchSize, timespan, unit, timer);
	}

	final static class SampleAction<T> extends BatchOperator.BatchAction<T, T> {

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
			return super.toString() + " sample=" + sample;
		}
	}
}
