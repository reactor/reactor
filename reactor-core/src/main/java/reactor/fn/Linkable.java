package reactor.fn;

/**
 * Simple abstraction to provide linking components together.
 *
 * @author Jon Brisbin
 */
public interface Linkable<T> {

	/**
	 * Link components together.
	 *
	 * @param t
	 * 		Array of components to link to this parent.
	 *
	 * @return {@literal this}
	 */
	Linkable<T> link(T t);

	/**
	 * Unlink components.
	 *
	 * @param t
	 * 		Component to unlink from this parent.
	 *
	 * @return {@literal this}
	 */
	Linkable<T> unlink(T t);

}
