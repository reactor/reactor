package reactor.net.udp.spec;

import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.support.EventRoutingComponentSpec;
import reactor.function.Consumer;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;
import reactor.net.NetChannel;
import reactor.net.config.ServerSocketOptions;
import reactor.net.udp.DatagramServer;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.*;

/**
 * @author Jon Brisbin
 */
public class DatagramServerSpec<IN, OUT>
		extends EventRoutingComponentSpec<DatagramServerSpec<IN, OUT>, DatagramServer<IN, OUT>> {

	protected final Constructor<? extends DatagramServer> serverImplCtor;

	private InetSocketAddress listenAddress;
	private ServerSocketOptions      options   = new ServerSocketOptions();
	private Collection<Consumer<IN>> consumers = Collections.emptyList();
	private Codec<Buffer, IN, OUT> codec;
	private NetworkInterface       multicastInterface;

	public DatagramServerSpec(Class<? extends DatagramServer> serverImpl) {
		Assert.notNull(serverImpl, "NetServer implementation class cannot be null.");
		try {
			this.serverImplCtor = serverImpl.getDeclaredConstructor(
					Environment.class,
					Reactor.class,
					InetSocketAddress.class,
					NetworkInterface.class,
					ServerSocketOptions.class,
					Codec.class,
					Collection.class
			);
			this.serverImplCtor.setAccessible(true);
		} catch(NoSuchMethodException e) {
			throw new IllegalArgumentException(
					"No public constructor found that matches the signature of the one found in the DatagramServer class.");
		}
	}


	/**
	 * Set the common {@link ServerSocketOptions} for connections made in this server.
	 *
	 * @param options
	 * 		The options to set when new connections are made.
	 *
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public DatagramServerSpec<IN, OUT> options(@Nonnull ServerSocketOptions options) {
		Assert.notNull(options, "ServerSocketOptions cannot be null.");
		this.options = options;
		return this;
	}

	/**
	 * The port on which this server should listen, assuming it should bind to all available addresses.
	 *
	 * @param port
	 * 		The port to listen on.
	 *
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public DatagramServerSpec<IN, OUT> listen(int port) {
		this.listenAddress = new InetSocketAddress(port);
		return this;
	}

	/**
	 * The {@link java.net.InetSocketAddress} on which this server should listen.
	 *
	 * @param listenAddress
	 * 		the specific {@link java.net.InetAddress} to bind to
	 * @param port
	 * 		the port to listen on
	 *
	 * @return {@literal this}
	 */
	public DatagramServerSpec<IN, OUT> listen(InetAddress listenAddress, int port) {
		this.listenAddress = new InetSocketAddress(listenAddress, port);
		return this;
	}

	/**
	 * The host and port on which this server should listen.
	 *
	 * @param host
	 * 		The host to bind to.
	 * @param port
	 * 		The port to listen on.
	 *
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public DatagramServerSpec<IN, OUT> listen(@Nullable String host, int port) {
		if(null == host) {
			host = "localhost";
		}
		this.listenAddress = new InetSocketAddress(host, port);
		return this;
	}

	/**
	 * The {@link Codec} to use to encode and decode data.
	 *
	 * @param codec
	 * 		The codec to use.
	 *
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public DatagramServerSpec<IN, OUT> codec(@Nonnull Codec<Buffer, IN, OUT> codec) {
		Assert.notNull(codec, "Codec cannot be null.");
		this.codec = codec;
		return this;
	}

	/**
	 * The {@link reactor.function.Consumer} to use to ingest incoming data.
	 *
	 * @param consumer
	 * 		the incoming data {@link reactor.function.Consumer}
	 *
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public DatagramServerSpec<IN, OUT> consume(Consumer<IN> consumer) {
		consume(Arrays.asList(consumer));
		return this;
	}

	/**
	 * The {@link reactor.function.Consumer Consumers} to use to ingest the incoming data.
	 *
	 * @param consumers
	 * 		the incoming data {@link reactor.function.Consumer Consumers}
	 *
	 * @return {@literal this}
	 */
	public DatagramServerSpec<IN, OUT> consume(Collection<Consumer<IN>> consumers) {
		Assert.notNull(consumers, "Consumers cannot be null");
		this.consumers = consumers;
		return this;
	}

	/**
	 * Set the interface to use for multicast.
	 *
	 * @param iface
	 * 		the {@link java.net.NetworkInterface} to use for multicast.
	 *
	 * @return {@literal this}
	 */
	public DatagramServerSpec<IN, OUT> multicastInterface(NetworkInterface iface) {
		this.multicastInterface = iface;
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected DatagramServer<IN, OUT> configure(Reactor reactor, Environment environment) {
		List<Consumer<NetChannel<IN, OUT>>> channelConsumers = new ArrayList<Consumer<NetChannel<IN, OUT>>>();
		channelConsumers.add(new Consumer<NetChannel<IN, OUT>>() {
			@Override
			public void accept(NetChannel<IN, OUT> netChannel) {
				for(Consumer<IN> consumer : consumers) {
					netChannel.consume(consumer);
				}
			}
		});

		try {
			return serverImplCtor.newInstance(
					environment,
					reactor,
					listenAddress,
					multicastInterface,
					options,
					codec,
					channelConsumers
			);
		} catch(Throwable t) {
			throw new IllegalStateException(t);
		}
	}

}
