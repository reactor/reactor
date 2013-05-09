package reactor.fn.dispatch;

/**
 * Components that implement this interface inform other components that it wants to be aware of any {@link Dispatcher}
 * it needs to use.
 *
 * @author Jon Brisbin
 */
public interface DispatcherAware {

	/**
	 * The {@link Dispatcher} currently in use.
	 *
	 * @return The {@link Dispatcher}.
	 */
	Dispatcher getDispatcher();

	/**
	 * Set the {@link Dispatcher} to use.
	 *
	 * @param dispatcher The {@link Dispatcher} to use.
	 * @return {@literal this}
	 */
	DispatcherAware setDispatcher(Dispatcher dispatcher);

}
