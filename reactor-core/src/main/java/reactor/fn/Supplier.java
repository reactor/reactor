package reactor.fn;

/**
 * Implementations of this class supply the caller with an object. The provided object can be created each call to
 * {@code get()} or can be created in some other way.
 *
 * @author Jon Brisbin
 */
public interface Supplier<T> {

	/**
	 * Get an object.
	 *
	 * @return An object.
	 */
	T get();

}
