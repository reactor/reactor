package reactor.alloc;

import java.util.List;

/**
 * An {@code Allocator} is responsible for returning to the caller a {@link reactor.alloc.Reference} to a reusable
 * object or to provide a newly-created object, depending on the underlying allocation strategy.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public interface Allocator<T extends Recyclable> {

	/**
	 * Allocate an object from the internal pool.
	 *
	 * @return a {@link reactor.alloc.Reference} that can be retained and released.
	 */
	Reference<T> allocate();

	/**
	 * Allocate a batch of objects all at once.
	 *
	 * @param size
	 * 		the number of objects to allocate
	 *
	 * @return a {@code List} of {@link Reference References} to the allocated objects
	 */
	List<Reference<T>> allocateBatch(int size);

	/**
	 * Efficiently release a batch of References all at once.
	 *
	 * @param batch
	 * 		the references to release
	 */
	void release(List<Reference<T>> batch);

}
