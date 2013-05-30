package reactor.fn.cache;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public interface Cache<T> {

	T allocate();

	void deallocate(T obj);

}
