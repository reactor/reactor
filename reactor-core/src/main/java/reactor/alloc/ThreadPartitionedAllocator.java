package reactor.alloc;

import reactor.function.Supplier;

/**
 * @author Jon Brisbin
 */
public class ThreadPartitionedAllocator<T extends Recyclable> extends PartitionedAllocator<T> {

	public ThreadPartitionedAllocator(Supplier<T> factory) {
		super(factory);
	}

	@Override
	protected long nextPartitionId() {
		return Thread.currentThread().getId();
	}

}
