package reactor.function.support;

import reactor.function.Consumer;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Jon Brisbin
 */
public class Resequencer<T> {

	private final ReentrantLock lock    = new ReentrantLock();
	private final AtomicLong    slots   = new AtomicLong(Long.MIN_VALUE);
	private final AtomicLong    claims  = new AtomicLong(Long.MIN_VALUE);
	private final Map<Long, T>  results = new TreeMap<Long, T>();
	private final Consumer<T> delegate;
	private final long        maxBacklog;

	public Resequencer(@Nonnull Consumer<T> delegate) {
		this(delegate, Integer.MAX_VALUE);
	}

	public Resequencer(@Nonnull Consumer<T> delegate, long maxBacklog) {
		this.delegate = delegate;
		this.maxBacklog = maxBacklog;
	}

	public void accept(@Nonnull Long slot, T t) {
		lock.lock();
		try {
			Assert.notNull(slot, "Slot cannot be null.");
			Assert.isTrue(slot <= slots.get(),
			              "Cannot accept a value for slot " + slot + " when only " + slots.get() + " slots have been " +
					              "allocated.");

			Long next = claims.incrementAndGet();
			if(slot.equals(next)) {
				delegate.accept(t);
				if(!results.isEmpty()) {
					for(Map.Entry<Long, T> entry : results.entrySet()) {
						delegate.accept(entry.getValue());
						claims.incrementAndGet();
					}
					results.clear();
				}
			} else {
				claims.decrementAndGet();
				Assert.isTrue(slot - claims.get() < maxBacklog, "Cannot backlog more than " + maxBacklog + " items.");
				results.put(slot, t);
			}
		} finally {
			lock.unlock();
		}
	}

	@Nonnull
	public Long next() {
		if(slots.incrementAndGet() < Long.MAX_VALUE) {
			return slots.get();
		} else {
			lock.lock();
			try {
				slots.set(Long.MIN_VALUE);
				return slots.get();
			} finally {
				lock.unlock();
			}
		}
	}

}
