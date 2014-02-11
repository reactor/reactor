package reactor.alloc.factory;

import reactor.function.Supplier;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple {@link reactor.function.Supplier} implementation that fills a fixed-size array with
 * pre-allocated objects, which are handed out, one after the other, until the array is exhausted.
 * At that point, the array is re-filled by creating more objects from the supplied factory and
 * those objects are then handed out one at a time. There is no limit to the number of items
 * created by the pool because references to the created objects are not maintained once it
 * has been retrieved from the pool.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public class BatchFactorySupplier<T> implements Supplier<T> {

	private static final AtomicIntegerFieldUpdater<BatchFactorySupplier> NEXT_UPD
			= AtomicIntegerFieldUpdater.newUpdater(BatchFactorySupplier.class, "next");

	private final ReentrantLock fillLock = new ReentrantLock(true);

	private final int         size;
	private final Supplier<T> factory;
	private final T[]         objs;

	private volatile int next = 0;

	@SuppressWarnings("unchecked")
	public BatchFactorySupplier(int size, Supplier<T> factory) {
		this.size = size;
		this.factory = factory;
		this.objs = (T[])new Object[size];
		for(int i = 0; i < size; i++) {
			objs[i] = factory.get();
		}
	}

	/**
	 * Returns the number of items being pooled.
	 *
	 * @return the size of the pool
	 */
	public int size() {
		return size;
	}

	/**
	 * How many unallocated items remain in the pool.
	 *
	 * @return the size of the unallocated pool
	 */
	public int remaining() {
		return size - next;
	}

	@Override
	public T get() {
		int next = NEXT_UPD.getAndIncrement(this);
		if(next >= size) {
			fill();
			return get();
		}

		T obj = objs[next];
		// wipe this reference
		objs[next] = null;

		return obj;
	}

	protected void fill() {
		fillLock.lock();
		try {
			// check again to see if the pool was filled by another thread
			if(this.next >= size) {
				for(int i = 0; i < size; i++) {
					objs[i] = factory.get();
				}
				NEXT_UPD.set(this, 0);
			}
		} finally {
			fillLock.unlock();
		}
	}

}
