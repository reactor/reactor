package reactor.core.composable;

import reactor.Fn;
import reactor.core.DispatcherComponentSpec;
import reactor.core.Reactor;
import reactor.function.Supplier;
import reactor.util.Assert;

/**
 * @author Jon Brisbin
 */
public class PromiseSpec<T> extends DispatcherComponentSpec<PromiseSpec<T>, Promise<T>> {

	private Composable<?> parent;
	private T             value;
	private Supplier<T>   valueSupplier;
	private Throwable     error;

	public PromiseSpec<T> link(Composable<?> parent) {
		this.parent = parent;
		return this;
	}

	public PromiseSpec<T> success(T value) {
		Assert.isNull(error, "Cannot set both a value and an error. Use one or the other.");
		Assert.isNull(valueSupplier, "Cannot set both a value and a Supplier. Use one or the other.");
		this.value = value;
		return this;
	}

	public PromiseSpec<T> success(Supplier<T> valueSupplier) {
		Assert.isNull(error, "Cannot set both an error and a Supplier. Use one or the other.");
		Assert.isNull(value, "Cannot set both a value and a Supplier. Use one or the other.");
		this.valueSupplier = valueSupplier;
		return this;
	}

	public PromiseSpec<T> error(Throwable error) {
		Assert.isNull(value, "Cannot set both a value and an error. Use one or the other.");
		Assert.isNull(valueSupplier, "Cannot set both an error and a Supplier. Use one or the other.");
		this.error = error;
		return this;
	}

	@Override
	protected Promise<T> configure(Reactor reactor) {
		return new Promise<T>(env, reactor, parent, (null != value ? Fn.supplier(value) : null), error, valueSupplier);
	}
}
