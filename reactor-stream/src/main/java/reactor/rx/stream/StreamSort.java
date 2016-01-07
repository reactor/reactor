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

import java.util.Comparator;
import java.util.PriorityQueue;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.support.ReactiveState;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.5
 */
public final class StreamSort<T> extends StreamBatch<T, T> {

	final private Comparator<? super T> comparator;

	public StreamSort(Publisher<T> source, int batchsize, Comparator<? super T> comparator) {
		super(source, batchsize, true, false, batchsize > 0);
		this.comparator = comparator;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new SortAction<>(prepareSub(subscriber), batchSize, comparator);
	}

	final static class SortAction<T> extends StreamBatch.BatchAction<T, T> {

		private final PriorityQueue<T> values;

		public SortAction(Subscriber<? super T> actual, int batchsize, Comparator<? super T> comparator) {
			super(actual, batchsize, true, false, batchsize > 0);
			if (comparator == null) {
				values = new PriorityQueue<T>();
			}
			else {
				values = new PriorityQueue<T>(
						batchsize > 0 && batchsize < Integer.MAX_VALUE ? batchsize : ReactiveState.SMALL_BUFFER_SIZE,
						comparator);
			}
		}

		@Override
		public void nextCallback(T value) {
			values.add(value);
		}

		@Override
		public void flushCallback(T ev) {
			if (values.isEmpty()) {
				return;
			}
			T value;
			while ((value = values.poll()) != null) {
				subscriber.onNext(value);
			}
		}

	}

}
