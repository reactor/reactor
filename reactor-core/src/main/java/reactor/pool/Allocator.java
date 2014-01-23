package reactor.pool;

/**
 * An {@code Allocator} is responsible for returning to the caller a {@link reactor.pool.Reference} to a reusable
 * object
 * or to provide a newly-created object, depending on the underlying allocation strategy.
 *
 * @author Jon Brisbin
 */
public interface Allocator<T extends Recyclable> {

	/**
	 * Allocate an object from the internal pool.
	 *
	 * @return a {@link reactor.pool.Reference} that can be retained and released.
	 */
	Reference<T> allocate();

}
