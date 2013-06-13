package reactor.tcp;

import reactor.Fn;
import reactor.core.*;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.registry.CachingRegistry;
import reactor.fn.registry.Registration;
import reactor.fn.registry.Registry;
import reactor.fn.selector.Selector;
import reactor.fn.tuples.Tuple2;
import reactor.io.Buffer;
import reactor.tcp.encoding.Codec;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.Iterator;

import static reactor.fn.Functions.$;

/**
 * @author Jon Brisbin
 */
public abstract class TcpClient<IN, OUT> {

	private final Tuple2<Selector, Object>         open        = $();
	private final Tuple2<Selector, Object>         close       = $();
	private final Registry<TcpConnection<IN, OUT>> connections = new CachingRegistry<TcpConnection<IN, OUT>>(null);

	private final Reactor                reactor;
	private final Codec<Buffer, IN, OUT> codec;

	protected final Environment env;

	protected TcpClient(@Nonnull Environment env,
											@Nonnull Reactor reactor,
											@Nonnull InetSocketAddress connectAddress,
											int rcvbuf,
											int sndbuf,
											@Nullable Codec<Buffer, IN, OUT> codec) {
		Assert.notNull(env, "A TcpClient cannot be created without a properly-configured Environment.");
		Assert.notNull(reactor, "A TcpClient cannot be created without a properly-configured Reactor.");
		Assert.notNull(connectAddress, "A TcpClient cannot be created without a properly-configure connect InetSocketAddress.");
		this.env = env;
		this.reactor = reactor;
		this.codec = codec;
	}

	/**
	 * Open a {@link TcpConnection} to the configured host:port and return a {@link Promise} that will be fulfilled when
	 * the client is connected.
	 *
	 * @return A {@link Promise} that will be filled with the {@link TcpConnection} when connected.
	 */
	public abstract Promise<TcpConnection<IN, OUT>> open();

	/**
	 * Close any open connections and disconnect this client.
	 *
	 * @return A {@link Promise} that will be fulfilled with {@literal null} when the connections have been closed.
	 */
	public Promise<Void> close() {
		Promise<Void> p = Promises.<Void>defer().using(env).using(reactor).get();
		Fn.schedule(
				new Consumer<Void>() {
					@Override
					public void accept(Void v) {
						for (Registration<? extends TcpConnection<IN, OUT>> reg : connections) {
							reg.getObject().close();
							reg.cancel();
						}
					}
				},
				null,
				reactor
		);
		return p;
	}

	/**
	 * Subclasses should register the given channel and connection for later use.
	 *
	 * @param channel    The channel object.
	 * @param connection The {@link TcpConnection}.
	 * @param <C>        The type of the channel object.
	 * @return {@link reactor.fn.registry.Registration} of this connection in the {@link Registry}.
	 */
	protected <C> Registration<? extends TcpConnection<IN, OUT>> register(@Nonnull C channel,
																																				@Nonnull TcpConnection<IN, OUT> connection) {
		Assert.notNull(channel, "Channel cannot be null.");
		Assert.notNull(connection, "TcpConnection cannot be null.");
		return connections.register($(channel), connection);
	}

	/**
	 * Find the {@link TcpConnection} for the given channel object.
	 *
	 * @param channel The channel object.
	 * @param <C>     The type of the channel object.
	 * @return The {@link TcpConnection} associated with the given channel.
	 */
	protected <C> TcpConnection<IN, OUT> select(@Nonnull C channel) {
		Assert.notNull(channel, "Channel cannot be null.");
		Iterator<Registration<? extends TcpConnection<IN, OUT>>> conns = connections.select(channel).iterator();
		if (conns.hasNext()) {
			return conns.next().getObject();
		} else {
			TcpConnection<IN, OUT> conn = createConnection(channel);
			register(channel, conn);
			notifyOpen(conn);
			return conn;
		}
	}

	/**
	 * Close the given channel.
	 *
	 * @param channel The channel object.
	 * @param <C>     The type of the channel object.
	 */
	protected <C> void close(@Nonnull C channel) {
		Assert.notNull(channel, "Channel cannot be null");
		for (Registration<? extends TcpConnection<IN, OUT>> reg : connections.select(channel)) {
			TcpConnection<IN, OUT> conn = reg.getObject();
			reg.getObject().close();
			notifyClose(conn);
			reg.cancel();
		}
	}

	/**
	 * Subclasses should implement this method and provide a {@link TcpConnection} object.
	 *
	 * @param channel The channel object to associate with this connection.
	 * @param <C>     The type of the channel object.
	 * @return The new {@link TcpConnection} object.
	 */
	protected abstract <C> TcpConnection<IN, OUT> createConnection(C channel);

	/**
	 * Notify this client's consumers than a global error has occurred.
	 *
	 * @param error The error to notify.
	 */
	protected void notifyError(@Nonnull Throwable error) {
		Assert.notNull(error, "Error cannot be null.");
		reactor.notify(error.getClass(), Event.wrap(error));
	}

	/**
	 * Notify this client's consumers that the connection has been opened.
	 *
	 * @param conn The {@link TcpConnection} that was opened.
	 */
	protected void notifyOpen(@Nonnull TcpConnection<IN, OUT> conn) {
		reactor.notify(open.getT2(), Event.wrap(conn));
	}

	/**
	 * Notify this clients's consumers that the connection has been closed.
	 *
	 * @param conn The {@link TcpConnection} that was closed.
	 */
	protected void notifyClose(@Nonnull TcpConnection<IN, OUT> conn) {
		reactor.notify(close.getT2(), Event.wrap(conn));
	}

	/**
	 * Get the {@link Codec} in use.
	 *
	 * @return The codec. May be {@literal null}.
	 */
	@Nullable
	protected Codec<Buffer, IN, OUT> getCodec() {
		return codec;
	}

	public static class Spec<IN, OUT> extends ComponentSpec<Spec<IN, OUT>, TcpClient<IN, OUT>> {
		private final Constructor<? extends TcpClient<IN, OUT>> clientImplConstructor;

		private InetSocketAddress connectAddress;
		private int rcvbuf = 8 * 1024;
		private int sndbuf = 8 * 1024;
		private Codec<Buffer, IN, OUT> codec;

		/**
		 * Create a {@code TcpClient.Spec} using the given implementation class.
		 *
		 * @param clientImpl The concrete implementation of {@link TcpClient} to instantiate.
		 */
		@SuppressWarnings({"unchecked", "rawtypes"})
		public Spec(@Nonnull Class<? extends TcpClient> clientImpl) {
			Assert.notNull(clientImpl, "TcpClient implementation class cannot be null.");
			try {
				this.clientImplConstructor = (Constructor<? extends TcpClient<IN, OUT>>) clientImpl.getDeclaredConstructor(
						Environment.class,
						Reactor.class,
						InetSocketAddress.class,
						int.class,
						int.class,
						Codec.class
				);
				this.clientImplConstructor.setAccessible(true);
			} catch (NoSuchMethodException e) {
				throw new IllegalArgumentException("No public constructor found that matches the signature of the one found in the TcpClient class.");
			}
		}

		/**
		 * Set the receive and send buffer sizes.
		 *
		 * @param rcvbuf The size of the receive buffer, in bytes.
		 * @param sndbuf The size of the send buffer, in bytes.
		 * @return {@literal this}
		 */
		public Spec<IN, OUT> bufferSize(int rcvbuf, int sndbuf) {
			this.rcvbuf = rcvbuf;
			this.sndbuf = sndbuf;
			return this;
		}

		/**
		 * The host and port to which this client should connect.
		 *
		 * @param host The host to connect to.
		 * @param port The port to connect to.
		 * @return {@literal this}
		 */
		public Spec<IN, OUT> connect(@Nonnull String host, int port) {
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
		public Spec<IN, OUT> codec(@Nullable Codec<Buffer, IN, OUT> codec) {
			Assert.isNull(this.codec, "Codec has already been set.");
			this.codec = codec;
			return this;
		}

		@Override
		protected TcpClient<IN, OUT> configure(Reactor reactor) {
			try {
				return clientImplConstructor.newInstance(
						env,
						reactor,
						connectAddress,
						rcvbuf,
						sndbuf,
						codec
				);
			} catch (Throwable t) {
				throw new IllegalStateException(t);
			}
		}
	}

}
