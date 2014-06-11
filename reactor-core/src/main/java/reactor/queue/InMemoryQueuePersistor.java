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
 * @author Stephane Maldini
 */
public class InMemoryQueuePersistor<T> implements QueuePersistor<T> {

	private final Map<Long, T> objects;
	private final AtomicLong   counter   = new AtomicLong();
	private final AtomicLong   currentId = new AtomicLong();

	public InMemoryQueuePersistor() {
		this.objects = Collections.synchronizedMap(new HashMap<Long, T>());
	}

	@Override
	public long lastId() {
		return currentId.get();
	}

	@Override
	public long size() {
		return counter.get();
	}

	@Override
	public boolean hasNext() {
		return counter.get() <= 0l;
	}

	@Override
	public Iterator<T> iterator() {
		return objects.values().iterator();
	}

	@Override
	public Long offer(@Nonnull T obj) {
		Long id = counter.getAndIncrement();
		objects.put(id, obj);
		return id;
	}

	@Override
	public T get(Long idx) {
		return objects.get(idx);
	}

	@Override
	public T remove() {
		Long id = currentId.getAndIncrement();
		counter.getAndDecrement();
		return objects.remove(id);
	}

	@Override
	public void close() {
	}
}
