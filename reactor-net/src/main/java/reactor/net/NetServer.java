package reactor.net;

import reactor.rx.Promise;

import javax.annotation.Nullable;

/**
 * A network-aware server.
 *
 * @author Jon Brisbin
 */
public interface NetServer<IN, OUT> extends Iterable<NetChannel<IN, OUT>> {

	/**
	 * Start and bind this {@literal NetServer} to the configured listen port.
	 *
	 * @return a {@link reactor.rx.Promise} that will be complete when the {@link NetServer} is started
	 */
	Promise<Boolean> start();

	/**
	 * Start and bind this {@literal NetServer} to the configured listen port and notify the given {@link
	 * reactor.function.Consumer} when the bind operation is complete.
	 *
	 * @param started
	 * 		{@link java.lang.Runnable} to invoke when bind operation is complete
	 *
	 * @return {@link this}
	 */
	NetServer<IN, OUT> start(@Nullable Runnable started);

	/**
	 * Shutdown this {@literal NetServer} and complete the returned {@link reactor.rx.Promise} when shut
	 * down.
	 *
	 * @return a {@link reactor.rx.Promise} that will be complete when the {@link NetServer} is shut down
	 */
	Promise<Boolean> shutdown();

}
