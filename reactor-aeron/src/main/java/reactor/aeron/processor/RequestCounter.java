/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.aeron.processor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks the number of requested items.
 * The total number of requested items of {@link Long#MAX_VALUE} means unlimited demand.
 *
 * @author Anatoly Kadyshev
 */
public class RequestCounter {

	private final long limit;

	private final AtomicLong requested = new AtomicLong(0);

	public RequestCounter(long limit) {
		this.limit = limit;
	}

	/**
	 * Increases the counter of requested items by <code>n</code>
	 *
	 * @param n number of items to request
	 */
	public void request(long n) {
		boolean success = true;
		do {
			long value = requested.get();
			if (value != Long.MAX_VALUE) {
				long newValue = value + n;
				if (newValue < 0) {
					success = requested.compareAndSet(value, Long.MAX_VALUE);
				} else {
					success = requested.compareAndSet(value, newValue);
				}
			}
		} while (!success);
	}

	/**
	 * Determines the next number of requested items capped by {@link #limit}
	 * The method should be called until it returns 0 which means no more items were requested.
	 *
	 * @return the number of requested items <= {@link #limit}
	 */
	public long getNextRequestLimit() {
		long l = requested.get();
		return l > limit ? limit : l;
	}

	/**
	 * Decreases the counter of requested items by <code>n</code>
	 *
	 * @param n number of items to release
	 */
	public void release(long n) {
		boolean success = true;
		do {
			long value = requested.get();
			if (value != Long.MAX_VALUE) {
				requested.compareAndSet(value, Math.max(value - n, 0));
			}
		} while(!success);
	}

}
