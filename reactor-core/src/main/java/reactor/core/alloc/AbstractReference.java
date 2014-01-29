package reactor.core.alloc;

import reactor.core.util.SystemUtils;

/**
 * @author Jon Brisbin
 */
public abstract class AbstractReference<T extends Recyclable> implements Reference<T> {

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
		refCnt -= Math.min(decr, refCnt);
		if(refCnt < 1) {
			obj.recycle();
		}
	}

	@Override
	public T get() {
		return obj;
	}

}
