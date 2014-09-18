/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public abstract class BatchAction<T, V> extends Action<T, V> {

	final boolean next;
	final boolean flush;
	final boolean first;
	final Consumer<T> flushConsumer = new FlushConsumer();
	long count = 0;

	public BatchAction(long batchSize,
	                   Dispatcher dispatcher, boolean next, boolean first, boolean flush) {
		super(dispatcher, batchSize);
		this.first = first;
		this.flush = flush;
		this.next = next;
	}

	protected void nextCallback(T event) {
	}

	protected void flushCallback(T event) {
	}

	protected void firstCallback(T event) {
	}

	@Override
	protected void doNext(T value) {
		count++;
		if (first && count == 1) {
			firstCallback(value);
		}

		if (next) {
			nextCallback(value);
		}

		if (flush && count % capacity == 0) {
			flushConsumer.accept(value);
		}
	}

	@Override
	protected void doComplete() {
		trySyncDispatch(null, flushConsumer);
		super.doComplete();
	}

	@Override
	public void available() {
		dispatch(null, flushConsumer);
		super.available();
	}

	@Override
	@SuppressWarnings("unchecked")
	public BatchAction<T,V> resume() {
		dispatch(null, flushConsumer);
		return (BatchAction<T,V>)super.resume();
	}

	@Override
	protected void requestUpstream(AtomicLong capacity, boolean terminated, long elements) {
		dispatch(null, flushConsumer);
		if(elements > this.capacity) {
			super.requestUpstream(capacity,
					terminated, elements);
		}else{
			super.requestUpstream(capacity,
					terminated, this.capacity - currentNextSignals > 0 ?
							this.capacity  :
							this.capacity);
		}
	}

	final private class FlushConsumer implements Consumer<T> {
		@Override
		public void accept(T n) {
			flushCallback(n);
			count = 0;
		}
	}

	@Override
	public String toString() {
		return super.toString()+"{batch: "+(count/capacity*100)+"% ("+count+")";
	}
}
