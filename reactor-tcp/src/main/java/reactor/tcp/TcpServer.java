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

import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.Promise;
import reactor.event.Event;
import reactor.event.registry.CachingRegistry;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.io.Buffer;
import reactor.tcp.config.ServerSocketOptions;
import reactor.tcp.config.SslOptions;
import reactor.tcp.encoding.Codec;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;

/**
 * Base functionality needed by all servers that communicate with clients over TCP.
 *
 * @param <IN> The type that will be received by this server
 * @param <OUT> The type that will be sent by this server
 *
 * @author Jon Brisbin
 */
public abstract class TcpServer<IN, OUT> {

	private final Event<TcpServer<IN, OUT>>        selfEvent   = Event.wrap(this);
	private final Tuple2<Selector, Object>         start       = Selectors.$();
	private final Tuple2<Selector, Object>         shutdown    = Selectors.$();
	private final Tuple2<Selector, Object>         open        = Selectors.$();
	private final Tuple2<Selector, Object>         close       = Selectors.$();
	private final Registry<TcpConnection<IN, OUT>> connections = new CachingRegistry<TcpConnection<IN, OUT>>();

	private final Reactor                reactor;
	private final Codec<Buffer, IN, OUT> codec;

	protected final Environment env;

	protected TcpServer(@Nonnull Environment env,
											@Nonnull Reactor reactor,
											@Nullable InetSocketAddress listenAddress,
											ServerSocketOptions options,
											SslOptions sslOptions,
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
		return connections.register(Selectors.$(channel), connection);
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

}
