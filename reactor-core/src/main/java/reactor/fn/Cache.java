package reactor.fn;

/**
 * @author Jon Brisbin
 */
public interface Cache<T> {

	T allocate();

	void deallocate(T obj);

}
