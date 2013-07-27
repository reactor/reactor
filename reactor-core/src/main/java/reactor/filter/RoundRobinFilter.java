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

package reactor.filter;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import reactor.util.Assert;

/**
 * A {@link Filter} implementation that returns a single item. The item is selected
 * using a round-robin algorithm based on the number of times the {@code key} has been
 * passed into the filter.
 *
 * @author Andy Wilkinson
 *
 */
public final class RoundRobinFilter extends AbstractFilter {

	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

	private final Lock readLock = readWriteLock.readLock();

	private final Lock writeLock = readWriteLock.writeLock();

	private final Map<Object, AtomicLong> usageCounts = new HashMap<Object, AtomicLong>();

	@Override
	public <T> List<T> doFilter(List<T> items, Object key) {
		Assert.notNull(key, "'key' must not be null");
		if (items.isEmpty()) {
			return items;
		} else {
			int index = (int)(getUsageCount(key).getAndIncrement() % (items.size()));
			return Collections.singletonList(items.get(index));
		}
	}

	private AtomicLong getUsageCount(Object key) {
		readLock.lock();
		try {
			AtomicLong usageCount = this.usageCounts.get(key);
			if (usageCount == null) {
				readLock.unlock();
				writeLock.lock();
				try {
					usageCount = this.usageCounts.get(key);
					if (usageCount == null) {
						usageCount = new AtomicLong();
						this.usageCounts.put(key, usageCount);
					}
				} finally {
					writeLock.unlock();
					readLock.lock();
				}
			}
			return usageCount;
		} finally {
			readLock.unlock();
		}
	}

}
