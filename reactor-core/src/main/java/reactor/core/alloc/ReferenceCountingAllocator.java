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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implementation of {@link Allocator} that uses reference counting to determine when an object
 * should
 * be recycled and placed back into the pool to be reused.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public class ReferenceCountingAllocator<T extends Recyclable> implements Allocator<T> {

	private static final int DEFAULT_INITIAL_SIZE = 2048;

	private final ReentrantLock           refLock    = new ReentrantLock();
	private final ReentrantLock           leaseLock  = new ReentrantLock();
	private final ArrayList<Reference<T>> references = new ArrayList<Reference<T>>();
	private final Supplier<T> factory;

	private volatile BitSet leaseMask;

	public ReferenceCountingAllocator(Supplier<T> factory) {
		this(DEFAULT_INITIAL_SIZE, factory);
	}

	public ReferenceCountingAllocator(int initialSize, Supplier<T> factory) {
		this.factory = factory;
		this.references.ensureCapacity(initialSize);
		this.leaseMask = new BitSet(initialSize);
		expand(initialSize);
	}

	@Override
	public Reference<T> allocate() {
		Reference<T> ref;
		int len = refCnt();
		int next;

		leaseLock.lock();
		try {
			next = leaseMask.nextClearBit(0);
			if (next >= len) {
				expand(len);
			}
			leaseMask.set(next);
		} finally {
			leaseLock.unlock();
		}

		if (next < 0) {
			throw new RuntimeException("Allocator is exhausted.");
		}

		ref = references.get(next);
		if (null == ref) {
			// this reference has been nulled somehow.
			// that's not really critical, just replace it.
			refLock.lock();
			try {
				ref = new ReferenceCountingAllocatorReference<T>(factory.get(), next);
				references.set(next, ref);
			} finally {
				refLock.unlock();
			}
		}
		ref.retain();

		return ref;
	}

	@Override
	public List<Reference<T>> allocateBatch(int size) {
		List<Reference<T>> refs = new ArrayList<Reference<T>>(size);
		for (int i = 0; i < size; i++) {
			refs.add(allocate());
		}
		return refs;
	}

	@Override
	public void release(List<Reference<T>> batch) {
		if (null != batch && !batch.isEmpty()) {
			for (Reference<T> ref : batch) {
				ref.release();
			}
		}
	}

	private int refCnt() {
		refLock.lock();
		try {
			return references.size();
		} finally {
			refLock.unlock();
		}
	}

	private void expand(int num) {
		refLock.lock();
		try {
			int len = references.size();
			int newLen = len + num;
			for (int i = len; i <= newLen; i++) {
				references.add(new ReferenceCountingAllocatorReference<T>(factory.get(), i));
			}
			BitSet newLeaseMask = new BitSet(newLen);
			int leases = leaseMask.length();
			for (int i = 0; i < leases; i++) {
				newLeaseMask.set(i, leaseMask.get(i));
			}
			leaseMask = newLeaseMask;
		} finally {
			refLock.unlock();
		}
	}

	private class ReferenceCountingAllocatorReference<T extends Recyclable> extends AbstractReference<T> {
		private final int bit;

		private ReferenceCountingAllocatorReference(T obj, int bit) {
			super(obj);
			this.bit = bit;
		}

		@Override
		public void release(int decr) {
			leaseLock.lock();
			try {
				super.release(decr);
				if (getReferenceCount() < 1) {
					// There won't be contention to clear this
					leaseMask.clear(bit);
				}
			} finally {
				leaseLock.unlock();
			}
		}
	}

}
