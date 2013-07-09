package reactor.tcp.spec;

import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.support.EventRoutingComponentSpec;
import reactor.io.Buffer;
import reactor.tcp.TcpClient;
import reactor.tcp.config.ClientSocketOptions;
import reactor.tcp.encoding.Codec;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;

/**
 * @author Jon Brisbin
 */
public class TcpClientSpec<IN, OUT> extends EventRoutingComponentSpec<TcpClientSpec<IN, OUT>, TcpClient<IN, OUT>> {

	private final Constructor<? extends TcpClient<IN, OUT>> clientImplConstructor;

	private InetSocketAddress connectAddress;
	private ClientSocketOptions options = new ClientSocketOptions();
	private Codec<Buffer, IN, OUT> codec;

	/**
	 * Create a {@code TcpClient.Spec} using the given implementation class.
	 *
	 * @param clientImpl The concrete implementation of {@link TcpClient} to instantiate.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public TcpClientSpec(@Nonnull Class<? extends TcpClient> clientImpl) {
		Assert.notNull(clientImpl, "TcpClient implementation class cannot be null.");
		try {
			this.clientImplConstructor = (Constructor<? extends TcpClient<IN, OUT>>) clientImpl.getDeclaredConstructor(
					Environment.class,
					Reactor.class,
					InetSocketAddress.class,
					ClientSocketOptions.class,
					Codec.class
			);
			this.clientImplConstructor.setAccessible(true);
		} catch (NoSuchMethodException e) {
			throw new IllegalArgumentException("No public constructor found that matches the signature of the one found in the TcpClient class.");
		}
	}


	/**
	 * Set the common {@link ClientSocketOptions} for connections made in this client.
	 *
	 * @param options The socket options to apply to new connections.
	 * @return {@literal this}
	 */
	public TcpClientSpec<IN, OUT> options(ClientSocketOptions options) {
		this.options = options;
		return this;
	}

	/**
	 * The host and port to which this client should connect.
	 *
	 * @param host The host to connect to.
	 * @param port The port to connect to.
	 * @return {@literal this}
	 */
	public TcpClientSpec<IN, OUT> connect(@Nonnull String host, int port) {
		Assert.isNull(connectAddress, "Connect address is already set.");
		this.connectAddress = new InetSocketAddress(host, port);
		return this;
	}

	/**
	 * The {@link Codec} to use to encode and decode data.
	 *
	 * @param codec The codec to use.
	 * @return {@literal this}
	 */
	public TcpClientSpec<IN, OUT> codec(@Nullable Codec<Buffer, IN, OUT> codec) {
		Assert.isNull(this.codec, "Codec has already been set.");
		this.codec = codec;
		return this;
	}

	@Override
	protected TcpClient<IN, OUT> configure(Reactor reactor, Environment env) {
		try {
			return clientImplConstructor.newInstance(
					env,
					reactor,
					connectAddress,
					options,
					codec
			);
		} catch (Throwable t) {
			throw new IllegalStateException(t);
		}
	}

}
