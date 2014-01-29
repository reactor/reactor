package reactor.core.alloc.factory;

/**
 * Helper class for creating object factories.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public abstract class Factories {

	private static final int DEFAULT_BATCH_SIZE = 2 * 1024;

	protected Factories() {
	}

	/**
	 * Create a {@link reactor.core.alloc.factory.BatchFactorySupplier} that supplies instances of the given type, which
	 * are created by invoked their no-arg constructor. An error will occur at creation if the type to be pooled has no
	 * zero-arg constructor (it can be private or protected; it doesn't have to be public, but it must exist).
	 *
	 * @param type
	 * 		the type to be pooled
	 * @param <T>
	 * 		the type of the pooled objects
	 *
	 * @return a {@link reactor.core.alloc.factory.BatchFactorySupplier} to supply instances of {@literal type}.
	 */
	public static <T> BatchFactorySupplier<T> create(Class<T> type) {
		return create(DEFAULT_BATCH_SIZE, type);
	}

	/**
	 * Create a {@link reactor.core.alloc.factory.BatchFactorySupplier} that supplies instances of the given type, which
	 * are created by invoked their no-arg constructor. An error will occur at creation if the type to be pooled has no
	 * zero-arg constructor (it can be private or protected; it doesn't have to be public, but it must exist).
	 *
	 * @param size
	 * 		size of the pool
	 * @param type
	 * 		the type to be pooled
	 * @param <T>
	 * 		the type of the pooled objects
	 *
	 * @return a {@link reactor.core.alloc.factory.BatchFactorySupplier} to supply instances of {@literal type}.
	 */
	public static <T> BatchFactorySupplier<T> create(int size, Class<T> type) {
		return new BatchFactorySupplier<T>(size, new NoArgConstructorFactory<T>(type));
	}

}
