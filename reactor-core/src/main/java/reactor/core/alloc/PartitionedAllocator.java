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

package reactor.core.alloc;

import reactor.fn.Supplier;
import reactor.jarjar.jsr166e.ConcurrentHashMapV8;

import java.util.List;

/**
 * An {@link Allocator} implementation that allocates a single object per thread, similar to a {@link
 * java.lang.ThreadLocal} but more efficient. Objects created here are never released.
 *
 * @author Jon Brisbin
 */
public abstract class PartitionedAllocator<T extends Recyclable> implements Allocator<T> {

	private final ConcurrentHashMapV8<Long, Reference<T>>     partitions = new ConcurrentHashMapV8<Long, Reference<T>>();
	private final ConcurrentHashMapV8.Fun<Long, Reference<T>> newRefFn   = new ConcurrentHashMapV8.Fun<Long, Reference<T>>() {
		@Override
		public Reference<T> apply(Long partId) {
			return new PartitionedReference<T>(factory.get(), partId);
		}
	};

	private final Supplier<T> factory;

	protected PartitionedAllocator(Supplier<T> factory) {
		this.factory = factory;
	}

	@Override
	public Reference<T> allocate() {
		long partId = nextPartitionId();
		Reference<T> ref;
		if (null != (ref = partitions.get(partId))
				|| null != (ref = partitions.computeIfAbsent(partId, newRefFn))) {
			return ref;
		}
		throw new IllegalStateException("Could not allocate from " + factory + " for thread " + Thread.currentThread());
	}

	@Override
	public List<Reference<T>> allocateBatch(int size) {
		throw new IllegalStateException("PartitionedAllocators don't allocate via batch");
	}

	@Override
	public void release(List<Reference<T>> batch) {
		for (Reference<T> ref : batch) {
			if (ref instanceof PartitionedReference) {
				long partId = ((PartitionedReference) ref).getPartitionId();
				partitions.get(partId).release();
			}
		}
	}

	protected abstract long nextPartitionId();

	private static class PartitionedReference<T extends Recyclable> extends AbstractReference<T> {
		private final long partitionId;

		public PartitionedReference(T obj, long partitionId) {
			super(obj);
			this.partitionId = partitionId;
		}

		public long getPartitionId() {
			return partitionId;
		}
	}

}
