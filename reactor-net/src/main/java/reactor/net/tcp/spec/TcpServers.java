package reactor.net.tcp.spec;

import reactor.core.Environment;
import reactor.net.tcp.TcpServer;

import javax.annotation.Nonnull;

/**
 * Helper class to make creating {@link reactor.net.tcp.TcpServer} instances more succinct.
 *
 * @author Jon Brisbin
 */
public abstract class TcpServers {

	protected TcpServers() {
	}

	/**
	 * Create a {@link reactor.net.tcp.spec.TcpServerSpec} for further configuration using the given {@link
	 * reactor.core.Environment} and {@code serverImpl}.
	 *
	 * @param env
	 * 		the {@link reactor.core.Environment} to use
	 * @param serverImpl
	 * 		the implementation of {@link reactor.net.tcp.TcpServer}
	 * @param <IN>
	 * 		type of the input
	 * @param <OUT>
	 * 		type of the output
	 *
	 * @return a {@link reactor.net.tcp.spec.TcpServerSpec} to be further configured
	 */
	public static <IN, OUT> TcpServerSpec<IN, OUT> create(Environment env,
	                                                      @Nonnull Class<? extends TcpServer> serverImpl) {
		return new TcpServerSpec<IN, OUT>(serverImpl).env(env);
	}

	/**
	 * Create a {@link reactor.net.tcp.spec.TcpServerSpec} for further configuration using the given {@link
	 * reactor.core.Environment} and {@code serverImpl}.
	 *
	 * @param env
	 * 		the {@link reactor.core.Environment} to use
	 * @param dispatcher
	 * 		the type of dispatcher to use
	 * @param serverImpl
	 * 		the implementation of {@link reactor.net.tcp.TcpServer}
	 * @param <IN>
	 * 		type of the input
	 * @param <OUT>
	 * 		type of the output
	 *
	 * @return a {@link reactor.net.tcp.spec.TcpServerSpec} to be further configured
	 */
	public static <IN, OUT> TcpServerSpec<IN, OUT> create(Environment env,
	                                                      String dispatcher,
	                                                      @Nonnull Class<? extends TcpServer> serverImpl) {
		return new TcpServerSpec<IN, OUT>(serverImpl).env(env).dispatcher(dispatcher);
	}

}
