package reactor.core.composable;

import reactor.core.DispatcherComponentSpec;
import reactor.core.Reactor;

/**
 * @author Jon Brisbin
 */
public class DeferredStreamSpec<T> extends DispatcherComponentSpec<DeferredStreamSpec<T>, Deferred<T, Stream<T>>> {

	private Composable<?> parent;
	private int batchSize = -1;
	private Iterable<T> values;

	public DeferredStreamSpec<T> link(Composable<?> parent) {
		this.parent = parent;
		return this;
	}

	public DeferredStreamSpec<T> batchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	public DeferredStreamSpec<T> each(Iterable<T> values) {
		this.values = values;
		return this;
	}

	@Override
	protected Deferred<T, Stream<T>> configure(Reactor reactor) {
		return new Deferred<T, Stream<T>>(new Stream<T>(env, reactor, batchSize, values, parent));
	}

}
