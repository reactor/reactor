package reactor.net;

import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.function.Consumer;

/**
 * A network-aware client.
 *
 * @author Jon Brisbin
 */
public interface NetClient<IN, OUT> {

	/**
	 * Open a channel to the configured address and return a {@link reactor.core.composable.Promise} that will be
	 * fulfilled with the connected {@link reactor.net.NetChannel}.
	 *
	 * @return {@link reactor.core.composable.Promise} that will be completed when connected
	 */
	Promise<NetChannel<IN, OUT>> open();

	/**
	 * Open a channel to the configured address and return a {@link reactor.core.composable.Stream} that will be
	 * populated
	 * by the {@link reactor.net.NetChannel NetChannels} every time a connection or reconnection is made.
	 *
	 * @param reconnect
	 * 		the reconnection strategy to use when disconnects happen
	 *
	 * @return
	 */
	Stream<NetChannel<IN, OUT>> open(Reconnect reconnect);

	/**
	 * Close this client and the underlying channel.
	 */
	Promise<Void> close();

	/**
	 * Close this client and the underlying channel and invoke the given {@link reactor.function.Consumer} when the
	 * operation has completed.
	 *
	 * @param onClose
	 * 		consumer to invoke when client is closed
	 */
	void close(Consumer<Void> onClose);

}
