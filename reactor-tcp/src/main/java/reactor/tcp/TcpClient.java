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
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.core.composable.spec.Promises;
import reactor.core.spec.Reactors;
import reactor.event.Event;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.io.Buffer;
import reactor.tcp.config.ClientSocketOptions;
import reactor.tcp.config.SslOptions;
import reactor.tcp.encoding.Codec;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The base class for a Reactor-based TCP client.
 *
 * @param <IN>
 * 		The type that will be received by this client
 * @param <OUT>
 * 		The type that will be sent by this client
 *
 * @author Jon Brisbin
 */
public abstract class TcpClient<IN, OUT> {

	private final Tuple2<Selector, Object> open  = Selectors.$();
	private final Tuple2<Selector, Object> close = Selectors.$();

	private final Reactor                reactor;
	private final Codec<Buffer, IN, OUT> codec;

	protected final Map<Object, WeakReference<TcpConnection<IN, OUT>>> connections = new ConcurrentHashMap<Object,
			WeakReference<TcpConnection<IN, OUT>>>();
	protected final Environment env;

	protected TcpClient(@Nonnull Environment env,
	                    @Nonnull Reactor reactor,
	                    @Nonnull InetSocketAddress connectAddress,
	                    @Nullable ClientSocketOptions options,
	                    @Nullable SslOptions sslOptions,
	                    @Nullable Codec<Buffer, IN, OUT> codec) {
		Assert.notNull(env, "A TcpClient cannot be created without a properly-configured Environment.");
		Assert.notNull(reactor, "A TcpClient cannot be created without a properly-configured Reactor.");
		Assert.notNull(connectAddress,
		               "A TcpClient cannot be created without a properly-configured connect InetSocketAddress.");
		this.env = env;
		this.reactor = reactor;
		this.codec = codec;
	}

	/**
	 * Open a {@link TcpConnection} to the configured host:port and return a {@link reactor.core.composable.Promise} that
	 * will be fulfilled when the client is connected.
	 *
	 * @return A {@link reactor.core.composable.Promise} that will be filled with the {@link TcpConnection} when
	 * connected.
	 */
	public abstract Promise<TcpConnection<IN, OUT>> open();

	/**
	 * Open a {@link TcpConnection} to the configured host:port and return a {@link Stream} that will be passed a new
	 * {@link TcpConnection} object every time the client is connected to the endpoint. The given {@link Reconnect}
	 * describes how the client should attempt to reconnect to the host if the initial connection fails or if the client
	 * successfully connects but at some point in the future gets cut off from the host. The {@code Reconnect} tells the
	 * client where to try reconnecting and gives a delay describing how long to wait to attempt to reconnect. When the
	 * connect is successfully made, the {@link Stream} is sent a new {@link TcpConnection} backed by the newly-connected
	 * connection.
	 *
	 * @param reconnect
	 *
	 * @return
	 */
	public abstract Stream<TcpConnection<IN, OUT>> open(Reconnect reconnect);

	/**
	 * Close any open connections and disconnect this client.
	 *
	 * @return A {@link Promise} that will be fulfilled with {@literal null} when the connections have been closed.
	 */
	public Promise<Void> close() {
		final Deferred<Void, Promise<Void>> d = Promises.defer(env, reactor.getDispatcher());
		Reactors.schedule(
				new Consumer<Void>() {
					@SuppressWarnings("ConstantConditions")
					@Override
					public void accept(Void v) {
						for(Map.Entry<Object, WeakReference<TcpConnection<IN, OUT>>> entry : connections.entrySet()) {
							if(null != entry.getValue().get()) {
								entry.getValue().get().close();
							}
						}
						doClose(d);
					}
				},
				null,
				reactor
		);
		return d.compose();
	}

	/**
	 * Subclasses should register the given channel and connection for later use.
	 *
	 * @param channel
	 * 		The channel object.
	 * @param connection
	 * 		The {@link TcpConnection}.
	 * @param <C>
	 * 		The type of the channel object.
	 *
	 * @return {@link reactor.event.registry.Registration} of this connection in the {@link Registry}.
	 */
	protected <C> Registration<? extends TcpConnection<IN, OUT>> register(@Nonnull final C channel,
	                                                                      @Nonnull final TcpConnection<IN,
			                                                                      OUT> connection) {
		Assert.notNull(channel, "Channel cannot be null.");
		Assert.notNull(connection, "TcpConnection cannot be null.");
		connections.put(channel, new WeakReference<TcpConnection<IN, OUT>>(connection));
		return new Registration<TcpConnection<IN, OUT>>() {
			@Override
			public Selector getSelector() {
				return null;
			}

			@Override
			public TcpConnection<IN, OUT> getObject() {
				return connection;
			}

			@Override
			public Registration<TcpConnection<IN, OUT>> cancelAfterUse() {
				return this;
			}

			@Override
			public boolean isCancelAfterUse() {
				return false;
			}

			@Override
			public Registration<TcpConnection<IN, OUT>> cancel() {
				connections.remove(channel);
				return this;
			}

			@Override
			public boolean isCancelled() {
				return !connections.containsKey(channel);
			}

			@Override
			public Registration<TcpConnection<IN, OUT>> pause() {
				return this;
			}

			@Override
			public boolean isPaused() {
				return false;
			}

			@Override
			public Registration<TcpConnection<IN, OUT>> resume() {
				return this;
			}
		};
	}

	/**
	 * Find the {@link TcpConnection} for the given channel object.
	 *
	 * @param channel
	 * 		The channel object.
	 * @param <C>
	 * 		The type of the channel object.
	 *
	 * @return The {@link TcpConnection} associated with the given channel.
	 */
	protected <C> TcpConnection<IN, OUT> select(@Nonnull C channel) {
		Assert.notNull(channel, "Channel cannot be null.");
		TcpConnection<IN, OUT> conn;
		if(!connections.containsKey(channel)) {
			conn = createConnection(channel);
			register(channel, conn);
			notifyOpen(conn);
			return conn;
		} else {
			conn = connections.get(channel).get();
		}
		return conn;
	}

	/**
	 * Close the given channel.
	 *
	 * @param channel
	 * 		The channel object.
	 * @param <C>
	 * 		The type of the channel object.
	 */
	protected <C> void close(@Nonnull C channel) {
		Assert.notNull(channel, "Channel cannot be null");
		WeakReference<TcpConnection<IN, OUT>> ref = connections.remove(channel);
		if(null != ref && null != ref.get()) {
			ref.get().close();
		}
	}

	/**
	 * Subclasses should implement this method and provide a {@link TcpConnection} object.
	 *
	 * @param channel
	 * 		The channel object to associate with this connection.
	 * @param <C>
	 * 		The type of the channel object.
	 *
	 * @return The new {@link TcpConnection} object.
	 */
	protected abstract <C> TcpConnection<IN, OUT> createConnection(C channel);

	/**
	 * Notify this client's consumers than a global error has occurred.
	 *
	 * @param error
	 * 		The error to notify.
	 */
	protected void notifyError(@Nonnull Throwable error) {
		Assert.notNull(error, "Error cannot be null.");
		reactor.notify(error.getClass(), Event.wrap(error));
	}

	/**
	 * Notify this client's consumers that the connection has been opened.
	 *
	 * @param conn
	 * 		The {@link TcpConnection} that was opened.
	 */
	protected void notifyOpen(@Nonnull TcpConnection<IN, OUT> conn) {
		reactor.notify(open.getT2(), Event.wrap(conn));
	}

	/**
	 * Notify this clients's consumers that the connection has been closed.
	 *
	 * @param conn
	 * 		The {@link TcpConnection} that was closed.
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

	protected abstract void doClose(Deferred<Void, Promise<Void>> d);

}
