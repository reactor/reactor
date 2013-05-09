package reactor.fn;

import java.util.concurrent.TimeUnit;

/**
 * Implementations of this interface can cause the caller to block in waiting for their actions to be performed.
 *
 * @author Jon Brisbin
 */
public interface Deferred<T> {

	/**
	 * Wait until the operation is complete and potentially return a non-null result.
	 *
	 * @return A result if available, {@literal null} otherwise.
	 * @throws InterruptedException
	 */
	T await() throws InterruptedException;

	/**
	 * Wait until the operation is complete for the specified timeout and potentially return a non-null result. If the
	 * timeout condition is reached, that is not considered an error condition and the return value may be null since the
	 * component may not have had time to populate the result.
	 *
	 * @param timeout The timeout value.
	 * @param unit    The unit of time to wait.
	 * @return A result if available, {@literal null} otherwise.
	 * @throws InterruptedException
	 */
	T await(long timeout, TimeUnit unit) throws InterruptedException;

}
