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

package reactor.util;

import com.gs.collections.api.block.function.Function0;
import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.api.block.procedure.Procedure2;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.api.map.MutableMap;
import com.gs.collections.api.set.ImmutableSet;
import com.gs.collections.api.set.MutableSet;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import reactor.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Jon Brisbin
 */
@ThreadSafe
public class PartitionedReferencePile<T> implements Supplier<T>, Iterable<T> {

	private final int size;
	private final Function0<MutableList<T>> preAllocatedListFn = new Function0<MutableList<T>>() {
		@Override
		public MutableList<T> value() {
			MutableList<T> vals = FastList.newList(size);
			for (int i = 0; i < size; i++) {
				vals.add(factory.get());
			}
			return vals;
		}
	};
	private final Supplier<T> factory;
	private final MutableMap<Long, MutableList<T>>     partitions      = UnifiedMap.newMap();
	private final MutableMap<Long, AtomicInteger>      nextAvailable   = UnifiedMap.newMap();
	private final Function0<AtomicInteger>             atomicIntegerFn = new Function0<AtomicInteger>() {
		@Override
		public AtomicInteger value() {
			return new AtomicInteger(-1);
		}
	};
	private final Procedure2<MutableList<T>, MutableList<T>> zipFn     = new Procedure2<MutableList<T>, MutableList<T>>() {
		@Override
		public void value(MutableList<T> vals, MutableList<T> agg) {
			agg.addAll(vals);
		}
	};

	public PartitionedReferencePile(Supplier<T> factory) {
		this(1024, factory);
	}

	public PartitionedReferencePile(int size, Supplier<T> factory) {
		this.size = size;
		this.factory = factory;
	}

	@Override
	public T get() {
		Long threadId = Thread.currentThread().getId();
		int nextAvail = nextAvailable.getIfAbsentPut(threadId, atomicIntegerFn).incrementAndGet();
		MutableList<T> vals = partitions.getIfAbsentPut(threadId, preAllocatedListFn);
		int len = vals.size();
		if (len == nextAvail) {
			vals.addAll(preAllocatedListFn.value());
		}

		return vals.get(nextAvail);
	}

	public ImmutableSet<T> collect() {
		final MutableSet<T> vals = UnifiedSet.newSet();
		partitions.keysView().forEach(new Procedure<Long>() {
			@Override
			public void value(Long threadId) {
				Iterator<T> iter = iteratorFor(threadId);
				while (iter.hasNext()) {
					vals.add(iter.next());
				}
			}
		});
		return vals.toImmutable();
	}

	public boolean isEmpty() {
		Long threadId = Thread.currentThread().getId();
		MutableList<T> vals = partitions.getIfAbsentPut(threadId, preAllocatedListFn);
		return !vals.isEmpty();
	}

	@Override
	public Iterator<T> iterator() {
		return iteratorFor(Thread.currentThread().getId());
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("PartitionedReferencePile{\n");
		partitions.forEachKeyValue(new Procedure2<Long, MutableList<T>>() {
			@Override
			public void value(Long threadId, MutableList<T> vals) {
				sb.append("\tthread:")
				  .append(threadId)
				  .append("=")
				  .append(vals.getFirst().getClass().getSimpleName())
				  .append("[").append(vals.size()).append("],\n");
			}
		});
		sb.append("}");
		return sb.toString();
	}

	private Iterator<T> iteratorFor(Long threadId) {
		AtomicInteger nextAvail = nextAvailable.getIfAbsentPut(threadId, atomicIntegerFn);
		final MutableList<T> vals = partitions.getIfAbsentPut(threadId, preAllocatedListFn);
		final int end = nextAvail.getAndSet(-1);

		return new Iterator<T>() {
			int currIdx = 0;

			@Override
			public boolean hasNext() {
				return currIdx <= end;
			}

			@Override
			public T next() {
				int idx = currIdx++;
				return vals.get(idx);
			}

			@Override
			public void remove() {
				throw new IllegalStateException("PartitionedReferencePile Iterators are read-only");
			}
		};
	}
}
