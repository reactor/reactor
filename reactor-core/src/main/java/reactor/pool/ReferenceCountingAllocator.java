package reactor.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.function.Supplier;
import reactor.util.Assert;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An implementation of {@link reactor.pool.Allocator} that uses reference counting to determine when an object should
 * be recycled and placed back into the pool to be reused.
 *
 * @author Jon Brisbin
 */
public class ReferenceCountingAllocator<T extends Recyclable> implements Allocator<T> {

	private static final int DEFAULT_INITIAL_SIZE = 2048;

	private final Logger                  log        = LoggerFactory.getLogger(getClass());
	private final ArrayList<Reference<T>> references = new ArrayList<Reference<T>>();
	private final ReentrantLock           leaseLock  = new ReentrantLock();
	private final Supplier<T> factory;
	private final BitSet      leaseMask;

	public ReferenceCountingAllocator(Supplier<T> factory) {
		this(DEFAULT_INITIAL_SIZE, factory);
	}

	public ReferenceCountingAllocator(int initialSize, Supplier<T> factory) {
		this.factory = factory;
		this.references.ensureCapacity(initialSize);
		this.leaseMask = new BitSet(initialSize);
		preallocate(0, initialSize);
	}

	@Override
	public Reference<T> allocate() {
		Reference<T> ref;
		int next = -1;
		leaseLock.lock();
		try {
			next = leaseMask.nextClearBit(0);
			if(next == references.size()) {
				preallocate(next, next);
			}
			leaseMask.set(next);
		} finally {
			leaseLock.unlock();
		}

		if(next < 0) {
			throw new RuntimeException("Allocator is exhausted.");
		}

		ref = references.get(next);
		ref.retain();

		return ref;
	}

	private void preallocate(int startIndex, int num) {
		int len = references.size();
		int newLen = len + num;
		for(int i = startIndex; i < newLen; i++) {
			references.add(new ReferenceCountingReference<T>(i, factory.get()));
		}
		if(log.isTraceEnabled()) {
			log.trace("Preallocated {} references starting at {}", num, startIndex);
		}
	}

	private class ReferenceCountingReference<T extends Recyclable> implements Reference<T> {
		private final AtomicInteger refCnt = new AtomicInteger(0);
		private final    int  bit;
		private final    long inception;
		private final    T    obj;
		private volatile long idleStart;

		private ReferenceCountingReference(int bit, T obj) {
			this.bit = bit;
			this.obj = obj;
			this.idleStart = this.inception = System.currentTimeMillis();
		}

		@Override
		public long getAge() {
			return System.currentTimeMillis() - inception;
		}

		@Override
		public long getIdleTime() {
			return (idleStart > 0 ? System.currentTimeMillis() - idleStart : idleStart);
		}

		@Override
		public int getReferenceCount() {
			return refCnt.get();
		}

		@Override
		public void retain() {
			retain(1);
		}

		@Override
		public void retain(int incr) {
			Assert.isTrue(incr > 0, "Cannot increment by zero or a negative amount");
			if(log.isTraceEnabled()) {
				log.trace("{}: retaining {} times", this, incr);
			}
			refCnt.addAndGet(incr);
		}

		@Override
		public void release() {
			release(1);
		}

		@Override
		public void release(int decr) {
			Assert.isTrue(decr > 0, "Cannot decrement by zero or a negative amount");
			if(log.isTraceEnabled()) {
				log.trace("{}: releasing {} times", this, decr);
			}
			int cnt = refCnt.addAndGet(-1 * decr);
			if(cnt < 1) {
				leaseLock.lock();
				try {
					leaseMask.clear(bit);
				} finally {
					leaseLock.unlock();
				}
				idleStart = System.currentTimeMillis();
				obj.recycle();
			}
		}

		@Override
		public T get() {
			idleStart = -1;
			return obj;
		}
	}

}
