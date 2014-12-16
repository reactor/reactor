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

package reactor.fn.support;

import reactor.core.support.Assert;
import reactor.fn.Consumer;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@code Resequencer} allows claimants to ensure proper ordering of replies by allocating {@code long} values from a
 * counter. When the claimant is ready to publish the results of the operation, it calls {@link #accept(long, Object)},
 * passing the slot number it claimed in addition to the value being published. The {@code Resequencer} will ensure
 * that out-of-order replies are re-ordered by the claimed slot number and later replies are queued and only passed to
 * the configured {@link reactor.fn.Consumer} once the earlier replies have been published.
 *
 * @author Jon Brisbin
 */
public class Resequencer<T> {

	private final ReentrantLock lock    = new ReentrantLock();
	private final AtomicLong    slots   = new AtomicLong();
	private final AtomicLong    claims  = new AtomicLong();
	private final Map<Long, T>  results = new TreeMap<Long, T>();
	private final Consumer<T> delegate;
	private final long        maxBacklog;

	/**
	 * Create a {@code Resequencer} that delegates to the given {@link reactor.fn.Consumer}.
	 *
	 * @param delegate
	 * 		the {@link reactor.fn.Consumer} to delegate values to.
	 */
	public Resequencer(@Nonnull Consumer<T> delegate) {
		this(delegate, Integer.MAX_VALUE);
	}

	/**
	 * Create a {@code Resequencer} that delegates to the given {@link reactor.fn.Consumer}. Only queue {@code
	 * maxBacklog} number of items before throwing an exception.
	 *
	 * @param delegate
	 * 		the {@link reactor.fn.Consumer} to delegate values to.
	 * @param maxBacklog
	 * 		the maximum number of items to queue in the backlog waiting on an earlier reply.
	 */
	public Resequencer(@Nonnull Consumer<T> delegate, long maxBacklog) {
		this.delegate = delegate;
		this.maxBacklog = maxBacklog;
	}

	/**
	 * Accept and possibly queue a value for the given {@code slot}.
	 *
	 * @param slot
	 * 		the slot id this value is a reply for.
	 * @param t
	 * 		the value to publish.
	 */
	public void accept(long slot, T t) {
		lock.lock();
		try {
			Assert.notNull(slot, "Slot cannot be null.");
			Assert.isTrue(slot <= slots.get(),
					"Cannot accept a value for slot " + slot + " when only " + slots.get() + " slots have been " +
							"allocated.");

			long next = claims.get() + 1;
			if (slot == next) {
				delegate.accept(t);
				claims.incrementAndGet();
				if (!results.isEmpty()) {
					for (Map.Entry<Long, T> entry : results.entrySet()) {
						delegate.accept(entry.getValue());
						claims.incrementAndGet();
					}
					results.clear();
				}
			} else {
				Assert.isTrue(slot - claims.get() < maxBacklog, "Cannot backlog more than " + maxBacklog + " items.");
				results.put(slot, t);
			}
		} finally {
			lock.unlock();
		}
	}

	public long next() {
		return slots.incrementAndGet();
	}

}
