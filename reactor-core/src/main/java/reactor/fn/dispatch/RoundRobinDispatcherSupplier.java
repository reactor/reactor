package reactor.fn.dispatch;

import reactor.fn.RoundRobinSupplier;

import java.util.List;

/**
 * @author Jon Brisbin
 */
public class RoundRobinDispatcherSupplier extends RoundRobinSupplier<Dispatcher> implements DispatcherSupplier {

	public RoundRobinDispatcherSupplier(Dispatcher... dispatchers) {
		super(dispatchers);
	}

	public RoundRobinDispatcherSupplier(List<Dispatcher> dispatchers) {
		super(dispatchers);
	}

}
