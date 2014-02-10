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
package reactor.core.action;

import reactor.core.Observable;
import reactor.event.Event;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Stephane Maldini
 */
public class BatchAction<T> extends Action<T>{

	protected final ReentrantLock lock = new ReentrantLock();

	private final int    batchSize;
	private final Object flushKey;
	private final Object firstKey;

	private volatile long errorCount  = 0l;
	private volatile long acceptCount = 0l;

	public BatchAction(int batchSize,
	                   Observable d,
	                   Object successKey,
	                   Object failureKey) {
		this(batchSize, d, successKey, failureKey, null, null);
	}

	public BatchAction(int batchSize,
	                   Observable d,
	                   Object successKey,
	                   Object failureKey,
	                   Object flushKey,
	                   Object firstKey) {
		super(d, successKey, failureKey);
		this.batchSize = batchSize;
		this.flushKey = flushKey;
		this.firstKey = firstKey;
	}

	public Object getFlushKey() {
		return flushKey;
	}

	public Object getFirstKey() {
		return firstKey;
	}

	public long getErrorCount() {
			return errorCount;
	}

	public long getAcceptCount() {
			return acceptCount;
	}

	public int getBatchSize() {
		return batchSize;
	}

	protected void doNext(Event<T> event) {
		if (getSuccessKey() != null) {
			notifyValue(event);
		}
	}

	protected void doFlush(Event<T> event) {
		if (flushKey != null) {
			getObservable().notify(flushKey, event);
		}
	}

	protected void doFirst(Event<T> event) {
		if (firstKey != null) {
			getObservable().notify(firstKey, event);
		}
	}

	@Override
	public void doAccept(Event<T> value) {
		if(batchSize == -1){
			doNext(value);
			return;
		}

		lock.lock();
		long accepted;
		try {
			accepted = (++acceptCount) % batchSize;

			if (accepted == 1) {
				doFirst(value);
			}

			doNext(value);

			if (accepted == 0) {
				doFlush(value);
			}
		} finally {
			lock.unlock();
		}

	}

	@Override
	protected void notifyError(Throwable error) {
		lock.lock();
		try {
			errorCount++;
			super.notifyError(error);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public String toString() {
		return super.toString()+"  % size:"+batchSize+" %  accepted:"+acceptCount+" % errors:"+errorCount;
	}
}
