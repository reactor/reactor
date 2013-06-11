package reactor.tcp;

import reactor.core.ComponentSpec;
import reactor.core.Environment;
import reactor.core.Reactor;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static reactor.Fn.$;

/**
 * Base functionality needed by all servers that communicate with clients over TCP.
 *
 * @author Jon Brisbin
 */
public abstract class TcpServer<IN, OUT> {

	private final Event<TcpServer<IN, OUT>>        selfEvent   = Event.wrap(this);
	private final Tuple2<Selector, Object>         start       = $();
	private final Tuple2<Selector, Object>         shutdown    = $();
	private final Tuple2<Selector, Object>         open        = $();
	private final Tuple2<Selector, Object>         close       = $();
	private final Registry<TcpConnection<IN, OUT>> connections = new CachingRegistry<TcpConnection<IN, OUT>>(null);

	private final Reactor                reactor;
	private final Codec<Buffer, IN, OUT> codec;

	protected final Environment env;

	protected TcpServer(@Nonnull Environment env,
											@Nonnull Reactor reactor,
											@Nullable InetSocketAddress listenAddress,
											int backlog,
											int rcvbuf,
											int sndbuf,
											@Nullable Codec<Buffer, IN, OUT> codec,
											@Nonnull Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers) {
		Assert.notNull(env, "A TcpServer cannot be created without a properly-configured Environment.");
		Assert.notNull(reactor, "A TcpServer cannot be created without a properly-configured Reactor.");
		this.env = env;
		this.reactor = reactor;
		this.codec = codec;

		Assert.notNull(connectionConsumers, "Connection Consumers cannot be null.");
		for (final Consumer<TcpConnection<IN, OUT>> consumer : connectionConsumers) {
			this.reactor.on(open.getT1(), new Consumer<Event<TcpConnection<IN, OUT>>>() {
				@Override
				public void accept(Event<TcpConnection<IN, OUT>> ev) {
					consumer.accept(ev.getData());
				}
			});
		}
	}

	/**
	 * Start this server.
	 *
	 * @return {@literal this}
	 */
	public TcpServer<IN, OUT> start() {
		return start(null);
	}

	/**
	 * Start this server, invoking the given callback when the server has started.
	 *
	 * @param started Callback to invoke when the server is started. May be {@literal null}.
	 * @return {@literal this}
	 */
	public abstract TcpServer<IN, OUT> start(@Nullable Consumer<Void> started);

	/**
	 * Shutdown this server.
	 *
	 * @return {@literal this}
	 */
	public TcpServer<IN, OUT> shutdown() {
		return shutdown(null);
	}

	/**
	 * Shutdown this server, invoking the given callback when the server has stopped.
	 *
	 * @param stopped Callback to invoke when the server is stopped. May be {@literal null}.
	 * @return {@literal this}
	 */
	public abstract TcpServer<IN, OUT> shutdown(@Nullable Consumer<Void> stopped);

	/**
	 * Subclasses should register the given channel and connection for later use.
	 *
	 * @param channel    The channel object.
	 * @param connection The {@link TcpConnection}.
	 * @param <C>        The type of the channel object.
	 * @return {@link Registration} of this connection in the {@link Registry}.
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
	 * Notify this server than a global error has occurred.
	 *
	 * @param error The error to notify.
	 */
	protected void notifyError(@Nonnull Throwable error) {
		Assert.notNull(error, "Error cannot be null.");
		reactor.notify(error.getClass(), Event.wrap(error));
	}

	/**
	 * Notify this server's consumers that the server has started.
	 *
	 * @param started An optional callback to invoke.
	 */
	protected void notifyStart(@Nullable final Consumer<Void> started) {
		if (null != started) {
			reactor.on(start.getT1(), new Consumer<Event<Void>>() {
				@Override
				public void accept(Event<Void> ev) {
					started.accept(null);
				}
			});
		}
		reactor.notify(start.getT2(), selfEvent);
	}

	/**
	 * Notify this server's consumers that the server has stopped.
	 *
	 * @param stopped An optional callback to invoke.
	 */
	protected void notifyShutdown(@Nullable final Consumer<Void> stopped) {
		if (null != stopped) {
			reactor.on(shutdown.getT1(), new Consumer<Event<Void>>() {
				@Override
				public void accept(Event<Void> ev) {
					stopped.accept(null);
				}
			});
		}
		reactor.notify(shutdown.getT2(), selfEvent);
	}

	/**
	 * Notify this server's consumers that the given connection has been opened.
	 *
	 * @param conn The {@link TcpConnection} that was opened.
	 */
	protected void notifyOpen(@Nonnull TcpConnection<IN, OUT> conn) {
		reactor.notify(open.getT2(), Event.wrap(conn));
	}

	/**
	 * Notify this server's consumers that the given connection has been closed.
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

	public static class Spec<IN, OUT> extends ComponentSpec<Spec<IN, OUT>, TcpServer<IN, OUT>> {
		private final Class<? extends TcpServer<IN, OUT>>       serverImpl;
		private final Constructor<? extends TcpServer<IN, OUT>> serverImplConstructor;

		private InetSocketAddress listenAddress;
		private int backlog = 512;
		private int rcvbuf  = 8 * 1024;
		private int sndbuf  = 8 * 1024;
		private Codec<Buffer, IN, OUT>                       codec;
		private Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers;

		/**
		 * Create a {@code TcpServer.Spec} using the given implementation class.
		 *
		 * @param serverImpl The concrete implementation of {@link TcpServer} to instantiate.
		 */
		@SuppressWarnings({"unchecked", "rawtypes"})
		public Spec(@Nonnull Class<? extends TcpServer> serverImpl) {
			Assert.notNull(serverImpl, "TcpServer implementation class cannot be null.");
			this.serverImpl = (Class<? extends TcpServer<IN, OUT>>) serverImpl;
			try {
				this.serverImplConstructor = (Constructor<? extends TcpServer<IN, OUT>>) serverImpl.getDeclaredConstructor(
						Environment.class,
						Reactor.class,
						InetSocketAddress.class,
						int.class,
						int.class,
						int.class,
						Codec.class,
						Collection.class
				);
				this.serverImplConstructor.setAccessible(true);
			} catch (NoSuchMethodException e) {
				throw new IllegalArgumentException("No public constructor found that matches the signature of the one found in the TcpServer class.");
			}
		}

		/**
		 * Set the value of the connection backlog.
		 *
		 * @param backlog The connection backlog.
		 * @return {@literal this}
		 */
		public Spec<IN, OUT> backlog(int backlog) {
			this.backlog = backlog;
			return this;
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
		 * The port on which this server should listen, assuming it should bind to all available addresses.
		 *
		 * @param port The port to listen on.
		 * @return {@literal this}
		 */
		public Spec<IN, OUT> listen(int port) {
			Assert.isNull(listenAddress, "Listen address is already set.");
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
		public Spec<IN, OUT> listen(@Nonnull String host, int port) {
			Assert.isNull(listenAddress, "Listen address is already set.");
			this.listenAddress = new InetSocketAddress(host, port);
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

		/**
		 * Callback to invoke when a new connection is created.
		 *
		 * @param connectionConsumer The callback to invoke for new connections.
		 * @return {@literal this}
		 */
		public Spec<IN, OUT> consume(@Nonnull Consumer<TcpConnection<IN, OUT>> connectionConsumer) {
			return consume(Collections.singletonList(connectionConsumer));
		}

		/**
		 * Callbacks to invoke when a new connection is created.
		 *
		 * @param connectionConsumers The callbacks to invoke for new connections.
		 * @return {@literal this}
		 */
		public Spec<IN, OUT> consume(@Nonnull Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers) {
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
						backlog,
						rcvbuf,
						sndbuf,
						codec,
						connectionConsumers
				);
			} catch (Throwable t) {
				throw new IllegalStateException(t);
			}
		}
	}

}
