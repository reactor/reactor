package reactor.fn;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Jon Brisbin
 */
public class RoundRobinSupplier<T> implements Supplier<T> {

	private final AtomicInteger counter = new AtomicInteger();
	private final T[]     objs;
	private final Integer count;

	public RoundRobinSupplier(T... objs) {
		this.count = objs.length;
		this.objs = objs;
	}

	@SuppressWarnings("unchecked")
	public RoundRobinSupplier(List<T> objs) {
		this.count = objs.size();
		this.objs = (T[]) objs.toArray();
	}

	@Override
	public T get() {
		int i = counter.incrementAndGet() % count;
		return objs[i];
	}

}
