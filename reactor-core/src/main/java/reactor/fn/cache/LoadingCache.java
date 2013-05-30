package reactor.fn.cache;

import reactor.fn.Supplier;
import reactor.support.QueueFactory;

import javax.annotation.Nullable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class LoadingCache<T> implements Cache<T> {

	private final BlockingQueue<T> cache = QueueFactory.createQueue();
	private final Supplier<T> supplier;
	private final int         softMax;
	private final long        cacheMissTimeout;
	private final AtomicLong leases = new AtomicLong();

	public LoadingCache(Supplier<T> supplier, int softMax, long cacheMissTimeout) {
		this.supplier = supplier;
		this.softMax = softMax;
		this.cacheMissTimeout = cacheMissTimeout;

		for (int i = 0; i < softMax; i++) {
			this.cache.add(supplier.get());
		}
	}

	@Nullable
	@Override
	public T allocate() {
		T obj;
		try {
			long start = System.currentTimeMillis();
			do {
				obj = cache.poll(cacheMissTimeout, TimeUnit.MILLISECONDS);
			} while (null == obj && (System.currentTimeMillis() - start) < cacheMissTimeout);
			leases.incrementAndGet();
			return (null != obj ? obj : supplier.get());
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return supplier.get();
		}
	}

	@Override
	public void deallocate(T obj) {
		//if (leases.decrementAndGet() < softMax) {
			cache.add(obj);
		//}
	}

}
