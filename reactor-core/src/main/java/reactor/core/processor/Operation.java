package reactor.core.processor;

import reactor.function.Supplier;

/**
 * @author Jon Brisbin
 */
public abstract class Operation<T> implements Supplier<T> {

	protected volatile Long id;
	private final      T    data;

	Operation(T data) {
		this.data = data;
	}

	void setId(Long id) {
		this.id = id;
	}

	@Override public T get() {
		return data;
	}

	public abstract void commit();

}
