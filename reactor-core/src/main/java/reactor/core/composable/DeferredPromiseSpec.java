package reactor.core.composable;

import reactor.core.DispatcherComponentSpec;
import reactor.core.Reactor;

/**
 * @author Jon Brisbin
 */
public class DeferredPromiseSpec<T> extends DispatcherComponentSpec<DeferredPromiseSpec<T>, Deferred<T, Promise<T>>> {

	private Composable<?> parent;

	public DeferredPromiseSpec<T> link(Composable<?> parent) {
		this.parent = parent;
		return this;
	}

	@Override
	protected Deferred<T, Promise<T>> configure(Reactor reactor) {
		return new Deferred<T, Promise<T>>(new Promise<T>(env, reactor, parent, null, null, null));
	}
}
