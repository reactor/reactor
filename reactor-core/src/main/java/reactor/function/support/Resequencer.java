package reactor.function.support;

import reactor.function.Consumer;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@code Resequencer} allows claimants to ensure proper ordering of replies by allocating {@code long} values from a
 * counter. When the claimant is ready to publish the results of the operation, it calls {@link #accept(Long, Object)},
 * passing the slot number it claimed in addition to the value being published. The {@code Resequencer} will ensure
 * that
 * out-of-order replies are re-ordered by the claimed slot number and later replies are queued and only passed to the
 * configured {@link reactor.function.Consumer} once the earlier replies have been published.
 *
 * @author Jon Brisbin
 */
public class Resequencer<T> {

	private final ReentrantLock lock    = new ReentrantLock();
	private final AtomicLong    slots   = new AtomicLong(Long.MIN_VALUE);
	private final AtomicLong    claims  = new AtomicLong(Long.MIN_VALUE);
	private final Map<Long, T>  results = new TreeMap<Long, T>();
	private final Consumer<T> delegate;
	private final long        maxBacklog;

	/**
	 * Create a {@code Resequencer} that delegates to the given {@link reactor.function.Consumer}.
	 *
	 * @param delegate
	 * 		the {@link reactor.function.Consumer} to delegate values to.
	 */
	public Resequencer(@Nonnull Consumer<T> delegate) {
		this(delegate, Integer.MAX_VALUE);
	}

	/**
	 * Create a {@code Resequencer} that delegates to the given {@link reactor.function.Consumer}. Only queue {@code
	 * maxBacklog} number of items before throwing an exception.
	 *
	 * @param delegate
	 * 		the {@link reactor.function.Consumer} to delegate values to.
	 * @param maxBacklog
	 * 		the maximum number of items to queue in the backlog waiting on an earlier reply.
	 */
	public Resequencer(@Nonnull Consumer<T> delegate, long maxBacklog) {
		this.delegate = delegate;
		this.maxBacklog = maxBacklog;
	}

	/**
	 * Accept and possibly queue a value for the given {@code slot}.
	 *
	 * @param slot
	 * 		the slot id this value is a reply for.
	 * @param t
	 * 		the value to publish.
	 */
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
