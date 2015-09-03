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

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class SortAction<T> extends BatchAction<T, T> {

	private final PriorityQueue<T> values;

	public SortAction(int batchsize, Comparator<? super T> comparator) {
		super(batchsize, true, false, batchsize > 0);
		if(comparator == null){
			values = new PriorityQueue<T>();
		}else{
			values = new PriorityQueue<T>(batchsize > 0 && batchsize < Integer.MAX_VALUE ? batchsize : 128, comparator);
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
		while((value = values.poll()) != null){
			broadcastNext(value);
		}
	}

}
