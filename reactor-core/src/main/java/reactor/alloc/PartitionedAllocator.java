package reactor.alloc;

import reactor.function.Supplier;
import reactor.jarjar.jsr166e.ConcurrentHashMapV8;

import java.util.List;

/**
 * An {@link reactor.alloc.Allocator} implementation that allocates a single object per thread, similar to a {@link
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
