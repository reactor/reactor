package reactor.core.alloc.factory;

import reactor.function.Supplier;

import java.lang.reflect.Constructor;

/**
 * A {@link reactor.function.Supplier} implementation that simply instantiates objects
 * using reflection from a no-arg constructor.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public class NoArgConstructorFactory<T> implements Supplier<T> {

	private final Class<T>       type;
	private final Constructor<T> ctor;

	public NoArgConstructorFactory(Class<T> type) {
		this.type = type;
		try {
			this.ctor = type.getConstructor();
			this.ctor.setAccessible(true);
		} catch(NoSuchMethodException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	@Override
	public T get() {
		try {
			return ctor.newInstance();
		} catch(Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

}
