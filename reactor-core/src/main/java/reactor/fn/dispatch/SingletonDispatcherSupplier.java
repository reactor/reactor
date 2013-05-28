package reactor.fn.dispatch;

/**
 * @author Jon Brisbin
 */
public class SingletonDispatcherSupplier implements DispatcherSupplier {

	private final Dispatcher dispatcher;

	public SingletonDispatcherSupplier(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	@Override
	public Dispatcher get() {
		return dispatcher;
	}

}
