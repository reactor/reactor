/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.rx.action.aggregation;

import java.util.Comparator;
import java.util.PriorityQueue;

import org.reactivestreams.Subscriber;
import reactor.core.processor.BaseProcessor;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class SortOperator<T> extends BatchOperator<T, T> {

	final private Comparator<? super T> comparator;

	public SortOperator(int batchsize, Comparator<? super T> comparator) {
		super(batchsize, true, false, batchsize > 0);
		this.comparator = comparator;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new SortAction<>(prepareSub(subscriber), batchSize, comparator);
	}

	final static class SortAction<T> extends BatchOperator.BatchAction<T, T> {

		private final PriorityQueue<T> values;

		public SortAction(Subscriber<? super T> actual, int batchsize, Comparator<? super T> comparator) {
			super(actual, batchsize, true, false, batchsize > 0);
			if (comparator == null) {
				values = new PriorityQueue<T>();
			}
			else {
				values = new PriorityQueue<T>(
						batchsize > 0 && batchsize < Integer.MAX_VALUE ? batchsize : BaseProcessor.SMALL_BUFFER_SIZE,
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
