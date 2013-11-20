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
package reactor.operations;

import reactor.core.Observable;
import reactor.event.Event;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Stephane Maldini
 */
public class BatchOperation<T> extends BaseOperation<T> {

	protected final ReentrantLock lock = new ReentrantLock();

	private final int    batchSize;
	private final Object flushKey;
	private final Object firstKey;
	private final Object lastKey;

	private volatile long errorCount  = 0l;
	private volatile long acceptCount = 0l;

	public BatchOperation(int batchSize, Observable d, Object successKey, Object failureKey, Object flushKey) {
		this(batchSize, d, successKey, failureKey, flushKey, null, null);
	}

	public BatchOperation(int batchSize, Observable d, Object successKey, Object failureKey, Object flushKey,
	                      Object firstKey, Object lastKey) {
		super(d, successKey, failureKey);
		this.batchSize = batchSize;
		this.flushKey = flushKey;
		this.lastKey = lastKey;
		this.firstKey = firstKey;
	}

	protected void notifyFlush() {
		getObservable().notify(flushKey, new Event<Void>(null));
	}

	protected void doFlush(Event<T> event) {
		notifyFlush();
	}

	protected void doNext(Event<T> event) {
		notifyValue(event);
	}

	@Override
	public void doOperation(Event<T> value) {
		lock.lock();
		try {
			long accepted = (++acceptCount) % batchSize;
			doNext(value);
			if (accepted == 1 && firstKey != null) {
				getObservable().notify(firstKey, value);
			} else if (accepted == 0) {
				if (lastKey != null) {
					getObservable().notify(lastKey, value);
				}
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

	public Object getFlushKey() {
		return flushKey;
	}

	public Object getFirstKey() {
		return firstKey;
	}

	public Object getLastKey() {
		return lastKey;
	}

	public long getErrorCount() {
		lock.lock();
		try {
			return errorCount;
		} finally {
			lock.unlock();
		}
	}

	public long getAcceptCount() {
		lock.lock();
		try {
			return acceptCount;
		} finally {
			lock.unlock();
		}
	}

	public int getBatchSize() {
		return batchSize;
	}
}
