package reactor.net.tcp.spec;

import reactor.core.Environment;
import reactor.net.tcp.TcpClient;

import javax.annotation.Nonnull;

/**
 * Helper class to make creating {@link reactor.net.tcp.TcpClient} instances more succinct.
 *
 * @author Jon Brisbin
 */
public class TcpClients {

	/**
	 * Create a {@link reactor.net.tcp.spec.TcpClientSpec} for further configuration using the given {@link
	 * reactor.core.Environment} and {@code clientImpl}.
	 *
	 * @param env
	 * 		the {@link reactor.core.Environment} to use
	 * @param clientImpl
	 * 		the {@link reactor.net.tcp.TcpClient} implementation to use
	 * @param <IN>
	 * 		type of the input
	 * @param <OUT>
	 * 		type of the output
	 *
	 * @return a new {@link reactor.net.tcp.spec.TcpClientSpec}
	 */
	public static <IN, OUT> TcpClientSpec<IN, OUT> create(Environment env,
	                                                      @Nonnull Class<? extends TcpClient> clientImpl) {
		return new TcpClientSpec<IN, OUT>(clientImpl).env(env);
	}

	/**
	 * Create a {@link reactor.net.tcp.spec.TcpClientSpec} for further configuration using the given {@link
	 * reactor.core.Environment}, {@link reactor.event.dispatch.Dispatcher} type, and {@code clientImpl}.
	 *
	 * @param env
	 * 		the {@link reactor.core.Environment} to use
	 * @param dispatcher
	 * 		the type of {@link reactor.event.dispatch.Dispatcher} to use
	 * @param clientImpl
	 * 		the {@link reactor.net.tcp.TcpClient} implementation to use
	 * @param <IN>
	 * 		type of the input
	 * @param <OUT>
	 * 		type of the output
	 *
	 * @return a new {@link reactor.net.tcp.spec.TcpClientSpec}
	 */
	public static <IN, OUT> TcpClientSpec<IN, OUT> create(Environment env,
	                                                      String dispatcher,
	                                                      @Nonnull Class<? extends TcpClient> clientImpl) {
		return new TcpClientSpec<IN, OUT>(clientImpl).env(env).dispatcher(dispatcher);
	}

}
