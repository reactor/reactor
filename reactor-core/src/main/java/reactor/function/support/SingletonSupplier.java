package reactor.function.support;

import reactor.function.Supplier;

/**
 * A simple {@link Supplier} implementation that always returns the given singleton.
 *
 * @author Jon Brisbin
 */
public class SingletonSupplier<T> implements Supplier<T> {

	private final T obj;

	/**
	 * Create a new {@link Supplier} to provide the given singleton.
	 *
	 * @param obj the singleton to provide
	 */
	public SingletonSupplier(T obj) {
		this.obj = obj;
	}

	/**
	 * Convenience method to create a {@link Supplier} from the given singleton.
	 *
	 * @param obj the singleton to provide
	 * @param <T> type of the singelton
	 * @return the new {@link Supplier}
	 */
	public static <T> Supplier<T> supply(T obj) {
		return new SingletonSupplier<T>(obj);
	}

	@Override
	public T get() {
		return obj;
	}

}
