package reactor.core.alloc.factory;

import reactor.function.Supplier;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Jon Brisbin
 */
public class FactoryAllocator<T> implements Supplier<T> {

	private static final AtomicIntegerFieldUpdater<FactoryAllocator> NEXT_UPD
			= AtomicIntegerFieldUpdater.newUpdater(FactoryAllocator.class, "next");

	private final ReentrantLock fillLock = new ReentrantLock(true);

	private final int         size;
	private final Supplier<T> factory;
	private final T[]         objs;

	private volatile int next = 0;

	@SuppressWarnings("unchecked")
	public FactoryAllocator(int size, Supplier<T> factory) {
		this.size = size;
		this.factory = factory;
		this.objs = (T[])new Object[size];
		for(int i = 0; i < size; i++) {
			objs[i] = factory.get();
		}
	}

	@Override
	public T get() {
		int next = NEXT_UPD.getAndIncrement(this);
		if(next >= size) {
			fill();
			next = NEXT_UPD.getAndIncrement(this);
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
