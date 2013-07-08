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

package reactor.cache;

import reactor.function.Supplier;
import reactor.support.BlockingQueueFactory;

import javax.annotation.Nullable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Cache} implementaion that uses a {@link Supplier} to create the cached objects.
 * The cache is preloaded with a configurable number of instances. In the event of an
 * object not being available to meet an allocation request a new object will be created.
 *
 * @param <T> The type of objects held by the cache
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class LoadingCache<T> implements Cache<T> {

	private final BlockingQueue<T> cache = BlockingQueueFactory.createQueue();
	private final Supplier<T> supplier;
	private final long        cacheMissTimeout;

	/**
	 * Creates a new {@code LoadingCache} that will use the {@code supplier} to
	 *
	 * @param supplier The Supplier used to create objects
	 * @param initial The number of objects to prepopulate the cache with
	 * @param cacheMissTimeout The period of time to wait for an object to be available before
	 *                         using the supplier to create a new one
	 */
	public LoadingCache(Supplier<T> supplier, int initial, long cacheMissTimeout) {
		this.supplier = supplier;
		this.cacheMissTimeout = cacheMissTimeout;

		for (int i = 0; i < initial; i++) {
			this.cache.add(supplier.get());
		}
	}

	@Nullable
	@Override
	public T allocate() {
		T obj;
		try {
			long start = System.currentTimeMillis();
			do {
				obj = cache.poll(cacheMissTimeout, TimeUnit.MILLISECONDS);
			} while (null == obj && (System.currentTimeMillis() - start) < cacheMissTimeout);
			return (null != obj ? obj : supplier.get());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return supplier.get();
		}
	}

	@Override
	public void deallocate(T obj) {
		cache.add(obj);
	}

}
