package reactor.core.alloc;

import reactor.function.Supplier;
import reactor.queue.BlockingQueueFactory;
import reactor.util.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author Jon Brisbin
 */
public class BlockingQueueAllocator<T extends Recyclable> implements Allocator<T> {

	private final BlockingQueue<Reference<T>> backlogQueue;
	private       int                         backlogSize;
	private final Supplier<T>                 factory;

	public BlockingQueueAllocator(int backlogSize, Supplier<T> factory) {
		this(backlogSize, BlockingQueueFactory.<Reference<T>>createQueue(), factory);
	}

	public BlockingQueueAllocator(int backlogSize,
	                              BlockingQueue<Reference<T>> backlogQueue,
	                              Supplier<T> factory) {
		this.backlogQueue = backlogQueue;
		this.backlogSize = (backlogSize > 0 ? backlogSize : 1024);
		Assert.notNull(factory, "Object factory cannot be null.");
		this.factory = factory;
		expand();
	}

	@Override
	public Reference<T> allocate() {
		Reference<T> obj;
		try {
			obj = backlogQueue.poll(200, TimeUnit.MILLISECONDS);
		} catch(InterruptedException e) {
			Thread.currentThread().interrupt();
			return null;
		}
		if(null == obj) {
			obj = new BlockingQueueReference(factory.get());
			expand();
		}
		return obj;
	}

	@Override
	public List<Reference<T>> allocateBatch(int size) {
		List<Reference<T>> batch = new ArrayList<Reference<T>>(size);
		for(int i = 0; i < size; i++) {
			Reference<T> ref = backlogQueue.poll();
			if(null == ref) {
				ref = new BlockingQueueReference(factory.get());
			}
			batch.add(ref);
		}
		return (batch.isEmpty() ? Collections.<Reference<T>>emptyList() : batch);
	}

	@Override
	public void release(List<Reference<T>> batch) {
		for(Reference<T> ref : batch) {
			ref.release();
		}
	}

	private void expand() {
		for(int i = 0; i < backlogSize; i++) {
			backlogQueue.add(new BlockingQueueReference(factory.get()));
		}
	}

	private class BlockingQueueReference extends AbstractReference<T> {
		private BlockingQueueReference(T obj) {
			super(obj);
		}

		@Override
		public void release(int decr) {
			super.release(decr);
			int refCnt = getReferenceCount();
			if(refCnt == 0 || (decr > 1 && refCnt < 0)) {
				backlogQueue.offer(this);
			}
		}
	}

}
