package reactor.util;

import com.gs.collections.api.block.function.Function0;
import com.gs.collections.api.block.procedure.Procedure2;
import com.gs.collections.api.map.MutableMap;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import reactor.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Jon Brisbin
 */
@ThreadSafe
public class PartitionedReferencePile<T> implements Supplier<T>, Iterable<T> {

	private final MutableMap<Long, FastList<T>>   partitions      = UnifiedMap.newMap();
	private final MutableMap<Long, AtomicInteger> nextAvailable   = UnifiedMap.newMap();
	private final Function0<AtomicInteger>        atomicIntegerFn = new Function0<AtomicInteger>() {
		@Override
		public AtomicInteger value() {
			return new AtomicInteger(-1);
		}
	};
	private final int size;
	private final Function0<FastList<T>> preAllocatedListFn = new Function0<FastList<T>>() {
		@Override
		public FastList<T> value() {
			FastList<T> vals = FastList.newList(size);
			for (int i = 0; i < size; i++) {
				vals.add(factory.get());
			}
			return vals;
		}
	};
	private final Supplier<T> factory;

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
		FastList<T> vals = partitions.getIfAbsentPut(threadId, preAllocatedListFn);
		int len = vals.size();
		if (len == nextAvail) {
			for (int i = 0; i < size; i++) {
				vals.add(factory.get());
			}
		}

		return vals.get(nextAvail);
	}

	@Override
	public Iterator<T> iterator() {
		Long threadId = Thread.currentThread().getId();
		final FastList<T> vals = partitions.remove(threadId);
		if (null == vals || vals.isEmpty()) {
			return Collections.<T>emptyList().iterator();
		}
		nextAvailable.remove(threadId);

		return new Iterator<T>() {
			int currIdx = 0;
			int len = vals.size();

			@Override
			public boolean hasNext() {
				return currIdx < len;
			}

			@Override
			public T next() {
				int idx = currIdx++;
				return vals.get(idx);
			}
		};
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("PartitionedReferencePile{\n");
		partitions.forEachKeyValue(new Procedure2<Long, FastList<T>>() {
			@Override
			public void value(Long threadId, FastList<T> vals) {
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

}
