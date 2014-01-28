package reactor.core.alloc;

import reactor.core.util.SystemUtils;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * @author Jon Brisbin
 */
public abstract class AbstractReference<T extends Recyclable> implements Reference<T> {

	private static final AtomicIntegerFieldUpdater<AbstractReference> REF_CNT_UPDATER
			= AtomicIntegerFieldUpdater.newUpdater(AbstractReference.class, "refCnt");

	private volatile int refCnt = 0;

	private final long inception;
	private final T    obj;

	protected AbstractReference(T obj) {
		this.obj = obj;
		this.inception = SystemUtils.approxCurrentTimeMillis();
	}

	@Override
	public long getAge() {
		return SystemUtils.approxCurrentTimeMillis() - inception;
	}

	@Override
	public long getIdleTime() {
		return -1;
	}

	@Override
	public int getReferenceCount() {
		return refCnt;
	}

	@Override
	public void retain() {
		retain(1);
	}

	@Override
	public void retain(int incr) {
		refCnt += incr;
	}

	@Override
	public void release() {
		release(1);
	}

	@Override
	public void release(int decr) {
		int cnt = (1 == decr ? --refCnt : (refCnt -= decr));
		if(cnt == 0) {
			obj.recycle();
		} else if(cnt < 1) {
			REF_CNT_UPDATER.set(this, 0);
		}
	}

	@Override
	public T get() {
		return obj;
	}

}
