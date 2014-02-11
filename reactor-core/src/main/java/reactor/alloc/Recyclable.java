package reactor.alloc;

/**
 * A simple interface that marks an object as being recyclable.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public interface Recyclable {

	/**
	 * Free any internal resources and reset the state of the object to enable reuse.
	 */
	void recycle();

}
