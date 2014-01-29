package reactor.core.alloc;

import reactor.function.Supplier;

/**
 * A {@code Reference} provides access to and metadata about a poolable object.
 *
 * @author Jon Brisbin
 */
public interface Reference<T extends Recyclable> extends Supplier<T> {

	/**
	 * Get the age of this {@code Reference} since it's creation.
	 *
	 * @return the number of milliseconds since this {@code Reference} was created.
	 */
	long getAge();

	/**
	 * Get the current number of references retained to this object.
	 *
	 * @return the reference count.
	 */
	int getReferenceCount();

	/**
	 * Increase reference count by {@literal 1}.
	 */
	void retain();

	/**
	 * Increase reference count by {@literal incr} amount.
	 *
	 * @param incr
	 * 		the amount to increment the reference count.
	 */
	void retain(int incr);

	/**
	 * Decrease the reference count by {@literal 1}.
	 */
	void release();

	/**
	 * Decrease the reference count by {@literal incr} amount.
	 *
	 * @param decr
	 * 		the amount to decrement the reference count.
	 */
	void release(int decr);

}
