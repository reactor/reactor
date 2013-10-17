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

package reactor.queue;

import reactor.function.Function;
import reactor.function.Supplier;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link QueuePersistor} implementations that stores items in-memory.
 *
 * @author Jon Brisbin
 */
public class InMemoryQueuePersistor<T> implements QueuePersistor<T> {

	private final Map<Long, T> objects   = Collections.synchronizedMap(new HashMap<Long, T>());
	private final AtomicLong   counter   = new AtomicLong();
	private final AtomicLong   currentId = new AtomicLong();
	private final Function<T, Long> offerFun;
	private final Function<Long, T> getFun;
	private final Supplier<T>       removeFun;

	public InMemoryQueuePersistor() {
		this.offerFun = new MapOfferFunction();
		this.getFun = new MapGetFunction();
		this.removeFun = new MapRemoveFunction();
	}

	@Override
	public long lastId() {
		return currentId.get();
	}

	@Override
	public long size() {
		return objects.size();
	}

	@Override
	public boolean hasNext() {
		return !objects.isEmpty();
	}

	@Override
	public Iterator<T> iterator() {
		return objects.values().iterator();
	}

	@Nonnull
	@Override
	public Function<T, Long> offer() {
		return offerFun;
	}

	@Nonnull
	@Override
	public Function<Long, T> get() {
		return getFun;
	}

	@Nonnull
	@Override
	public Supplier<T> remove() {
		return removeFun;
	}

	@Override
	public void close() {
	}

	private class MapOfferFunction implements Function<T, Long> {
		@Override
		public Long apply(T obj) {
			Long id = counter.getAndIncrement();
			objects.put(id, obj);
			return id;
		}
	}

	private class MapGetFunction implements Function<Long, T> {
		@Override
		public T apply(Long l) {
			return objects.get(l);
		}
	}

	private class MapRemoveFunction implements Supplier<T> {
		@Override
		public T get() {
			Long id = currentId.getAndIncrement();
			return objects.remove(id);
		}
	}

}
