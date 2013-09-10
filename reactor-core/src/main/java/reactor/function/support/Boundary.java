package reactor.function.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import reactor.function.Consumer;

/**
 * A {@code Boundary} is a blocking utility that allows the user to bind an arbitrary number of {@link Consumer
 * Consumers} to it. Whenever {@link #bind(reactor.function.Consumer)} is called, it returns a new {@code Consumer}
 * that
 * internally creates a {@link CountDownLatch} which calls the delegate {@code Consumer}, then counts down the latch.
 * Calling {@link #await()} or {@link #await(long, java.util.concurrent.TimeUnit)} on the {@code Boundary} will block
 * the calling thread until all bound latches are released.
 * <p/>
 * The timeout value given is the total timeout value and not the per-latch timeout value. If a timeout of 5 seconds is
 * specified and there are 100 latches bound, then all 100 latches have a combined 5 seconds to be counted down, not 5
 * seconds per latch, which would equate to a little under 10 minutes.
 *
 * @author Jon Brisbin
 */
public class Boundary {

	private final List<CountDownLatch> latches = new ArrayList<CountDownLatch>();

	public Boundary() {
	}

	/**
	 * Bind the given {@link Consumer} to this {@code Boundary} by creating a {@link CountDownLatch} that will be counted
	 * down the first time the given {@link Consumer} is invoked.
	 *
	 * @param consumer
	 * 		The delegate {@code Consumer}
	 * @param <T>
	 * 		The type of the value accepted by the {@code Consumer}
	 *
	 * @return A new {@code Consumer} which will count down the internal latch after invoking the delegate {@code
	 * Consumer}
	 */
	public <T> Consumer<T> bind(Consumer<T> consumer) {
		return bind(consumer, 1);
	}

	/**
	 * Bind the given {@link Consumer} to this {@code Boundary} by creating a {@link CountDownLatch} of the given size
	 * that will be counted down the every time the given {@link Consumer} is invoked.
	 *
	 * @param consumer
	 * 		The delegate {@code Consumer}
	 * @param <T>
	 * 		The type of the value accepted by the {@code Consumer}
	 *
	 * @return A new {@code Consumer} which will count down the internal latch after invoking the delegate {@code
	 * Consumer}
	 */
	public <T> Consumer<T> bind(final Consumer<T> consumer, int expected) {
		synchronized(latches) {
			final CountDownLatch latch = new CountDownLatch(expected);
			latches.add(latch);

			return new Consumer<T>() {
				@Override
				public void accept(T t) {
					consumer.accept(t);
					latch.countDown();
				}
			};
		}
	}

	/**
	 * Wait for all latches to be counted down (almost) indefinitely.
	 *
	 * @return {@code true} if all latches were counted down within the timeout value, {@code false} otherwise
	 */
	public boolean await() {
		return await(Integer.MAX_VALUE, TimeUnit.SECONDS);
	}

	/**
	 * Wait for all latches to be counted down within the given timeout window.
	 *
	 * @param timeout
	 * 		The timeout value.
	 * @param timeUnit
	 * 		The unit of time measured by the timeout value.
	 *
	 * @return {@code true} if all latches were counted down within the timeout value, {@code false} otherwise
	 */
	public boolean await(long timeout, TimeUnit timeUnit) {
		if(latches.isEmpty()) {
			// we're not watching any latches
			return true;
		}

		final long start = System.currentTimeMillis();
		final long timeoutMillis = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
		synchronized(latches) {
			try {
				long elapsed = 0;
				for(CountDownLatch latch : latches) {
					boolean b = latch.await(timeoutMillis - elapsed, TimeUnit.MILLISECONDS);
					elapsed = System.currentTimeMillis() - start;
					if(!b || elapsed >= timeoutMillis) {
						return false;
					}
				}
			} catch(InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		return (System.currentTimeMillis() - start < timeoutMillis);
	}

}
