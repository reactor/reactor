package reactor.tcp.test;

/**
 * Utility methods for working with timeouts
 *
 * @author Andy Wilkinson
 *
 */
public final class TimeoutUtils {

	private TimeoutUtils() {

	}

	/**
	 * Polls the given {@code callback} until it is {@link TimeoutCallback#isComplete()} or {@code timeout}
	 * milliseconds have elapsed. If the timeout is reached, an {@code IllegalStateException} is thrown.
	 *
	 * @param timeout The timeout period, in milliseconds
	 * @param callback The callback to poll
	 *
	 * @throws InterruptedException if interrupted
	 * @throws IllegalStateException if
	 */
	public static void doWithTimeout(long timeout, TimeoutCallback callback) throws InterruptedException {
		long endTime = System.currentTimeMillis() + timeout;

		while (!callback.isComplete() && System.currentTimeMillis() < endTime) {
			Thread.sleep(100);
		}

		if (!callback.isComplete()) {
			throw new IllegalStateException("Operation was not complete within " + timeout + "ms");
		}
	}

	/**
	 * A callback to be polled for completion of an operation.
	 *
	 * @author Andy Wilkinson
	 */
	public interface TimeoutCallback {

		/**
		 * Indicates whether or not the operation has completed
		 *
		 * @return {@code true} if the operation is complete, otherwise {@code false}.
		 */
		boolean isComplete();

	}

}
