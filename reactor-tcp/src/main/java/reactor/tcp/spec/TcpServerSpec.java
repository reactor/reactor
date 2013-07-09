package reactor.tcp.spec;

import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.support.EventRoutingComponentSpec;
import reactor.function.Consumer;
import reactor.io.Buffer;
import reactor.tcp.TcpConnection;
import reactor.tcp.TcpServer;
import reactor.tcp.config.ServerSocketOptions;
import reactor.tcp.encoding.Codec;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;

/**
 * @author Jon Brisbin
 */
public class TcpServerSpec<IN, OUT> extends EventRoutingComponentSpec<TcpServerSpec<IN, OUT>, TcpServer<IN, OUT>> {

	private final Constructor<? extends TcpServer<IN, OUT>> serverImplConstructor;

	private InetSocketAddress   listenAddress = new InetSocketAddress("localhost", 3000);
	private ServerSocketOptions options       = new ServerSocketOptions();
	private Codec<Buffer, IN, OUT>                       codec;
	private Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers;

	/**
	 * Create a {@code TcpServer.Spec} using the given implementation class.
	 *
	 * @param serverImpl The concrete implementation of {@link TcpServer} to instantiate.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public TcpServerSpec(@Nonnull Class<? extends TcpServer> serverImpl) {
		Assert.notNull(serverImpl, "TcpServer implementation class cannot be null.");
		try {
			this.serverImplConstructor = (Constructor<? extends TcpServer<IN, OUT>>) serverImpl.getDeclaredConstructor(
					Environment.class,
					Reactor.class,
					InetSocketAddress.class,
					ServerSocketOptions.class,
					Codec.class,
					Collection.class
			);
			this.serverImplConstructor.setAccessible(true);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException("No public constructor found that matches the signature of the one found in the TcpServer class.");
		}
	}

	/**
	 * Set the common {@link ServerSocketOptions} for connections made in this server.
	 *
	 * @param options The options to set when new connections are made.
	 * @return {@literal this}
	 */
	public TcpServerSpec<IN, OUT> options(@Nonnull ServerSocketOptions options) {
		Assert.notNull(options, "ServerSocketOptions cannot be null.");
		this.options = options;
		return this;
	}

	/**
	 * The port on which this server should listen, assuming it should bind to all available addresses.
	 *
	 * @param port The port to listen on.
	 * @return {@literal this}
	 */
	public TcpServerSpec<IN, OUT> listen(int port) {
		this.listenAddress = new InetSocketAddress(port);
		return this;
	}

	/**
	 * The host and port on which this server should listen.
	 *
	 * @param host The host to bind to.
	 * @param port The port to listen on.
	 * @return {@literal this}
	 */
	public TcpServerSpec<IN, OUT> listen(@Nonnull String host, int port) {
		if (null == host) {
			host = "localhost";
		}
		this.listenAddress = new InetSocketAddress(host, port);
		return this;
	}

	/**
	 * The {@link Codec} to use to encode and decode data.
	 *
	 * @param codec The codec to use.
	 * @return {@literal this}
	 */
	public TcpServerSpec<IN, OUT> codec(@Nonnull Codec<Buffer, IN, OUT> codec) {
		Assert.notNull(codec, "Codec cannot be null.");
		this.codec = codec;
		return this;
	}

	/**
	 * Callback to invoke when a new connection is created.
	 *
	 * @param connectionConsumer The callback to invoke for new connections.
	 * @return {@literal this}
	 */
	public TcpServerSpec<IN, OUT> consume(@Nonnull Consumer<TcpConnection<IN, OUT>> connectionConsumer) {
		return consume(Collections.singletonList(connectionConsumer));
	}

	/**
	 * Callbacks to invoke when a new connection is created.
	 *
	 * @param connectionConsumers The callbacks to invoke for new connections.
	 * @return {@literal this}
	 */
	public TcpServerSpec<IN, OUT> consume(@Nonnull Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers) {
		Assert.notNull(connectionConsumers, "Connection consumers cannot be null.");
		this.connectionConsumers = connectionConsumers;
		return this;
	}

	@Override
	protected TcpServer<IN, OUT> configure(Reactor reactor) {
		try {
			return serverImplConstructor.newInstance(
					env,
					reactor,
					listenAddress,
					options,
					codec,
					connectionConsumers
			);
		} catch (Throwable t) {
			throw new IllegalStateException(t);
		}
	}

}
