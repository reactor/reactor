/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.tcp;

import static reactor.fn.Functions.$;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import reactor.core.ComponentSpec;
import reactor.core.Environment;
import reactor.core.Promise;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.registry.CachingRegistry;
import reactor.fn.registry.Registration;
import reactor.fn.registry.Registry;
import reactor.fn.selector.Selector;
import reactor.fn.tuples.Tuple2;
import reactor.io.Buffer;
import reactor.tcp.config.ServerSocketOptions;
import reactor.tcp.encoding.Codec;
import reactor.util.Assert;

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
											ServerSocketOptions options,
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
	 * @return a Promise that will be completed with the shutdown outcome
	 */
	public abstract Promise<Void> shutdown();

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
	 */
	protected void notifyShutdown() {
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

	protected Reactor getReactor() {
		return this.reactor;
	}

	public static class Spec<IN, OUT> extends ComponentSpec<Spec<IN, OUT>, TcpServer<IN, OUT>> {
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
		public Spec(@Nonnull Class<? extends TcpServer> serverImpl) {
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
		public Spec<IN, OUT> options(@Nonnull ServerSocketOptions options) {
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
		public Spec<IN, OUT> listen(int port) {
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
		public Spec<IN, OUT> codec(@Nonnull Codec<Buffer, IN, OUT> codec) {
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

}
