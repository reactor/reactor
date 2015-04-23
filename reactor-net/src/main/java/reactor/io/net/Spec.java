/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.io.net;

import reactor.Environment;
import reactor.bus.spec.DispatcherComponentSpec;
import reactor.core.Dispatcher;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.fn.Suppliers;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.config.ClientSocketOptions;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.http.HttpChannel;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.LinkedList;
import java.util.List;

/**
 * Specifications used to build client and servers.
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 2.0
 */
public interface Spec {

	public static final Function NOOP_DECODER = new Function() {
		@Override
		public Object apply(Object o) {
			return o;
		}
	};

	public static final Codec NOOP_CODEC = new Codec() {
		@Override
		public Function decoder(Consumer next) {
			return NOOP_DECODER;
		}

		@Override
		public Object apply(Object o) {
			return o;
		}
	};

	//
	//   Client and Server Specifications
	//
	abstract static class ServerSpec<IN, OUT,
			CONN extends Channel<IN, OUT>,
			S extends ServerSpec<IN, OUT, CONN, S, N>,
			N extends reactor.io.net.Server<IN, OUT, CONN>>
			extends DispatcherComponentSpec<S, N> {

		protected ServerSocketOptions options = new ServerSocketOptions();
		protected InetSocketAddress      listenAddress;
		protected Codec<Buffer, IN, OUT> codec;

		/**
		 * Set the common {@link reactor.io.net.config.ServerSocketOptions} for channels made in this server.
		 *
		 * @param options The options to set when new channels are made.
		 * @return {@literal this}
		 */
		@SuppressWarnings("unchecked")
		public S options(@Nonnull ServerSocketOptions options) {
			Assert.notNull(options, "ServerSocketOptions cannot be null.");
			this.options = options;
			return (S) this;
		}

		/**
		 * The port on which this server should listen, assuming it should bind to all available addresses.
		 *
		 * @param port The port to listen on.
		 * @return {@literal this}
		 */
		@SuppressWarnings("unchecked")
		public S listen(int port) {
			return listen(new InetSocketAddress(port));
		}

		/**
		 * The host and port on which this server should listen.
		 *
		 * @param host The host to bind to.
		 * @param port The port to listen on.
		 * @return {@literal this}
		 */
		@SuppressWarnings("unchecked")
		public S listen(String host, int port) {
			if (null == host) {
				host = "localhost";
			}
			return listen(new InetSocketAddress(host, port));
		}

		/**
		 * The {@link java.net.InetSocketAddress} on which this server should listen.
		 *
		 * @param listenAddress the listen address
		 * @return {@literal this}
		 */
		@SuppressWarnings("unchecked")
		public S listen(InetSocketAddress listenAddress) {
			this.listenAddress = listenAddress;
			return (S) this;
		}

		/**
		 * The {@link reactor.io.codec.Codec} to use to encode and decode data.
		 *
		 * @param codec The codec to use.
		 * @return {@literal this}
		 */
		@SuppressWarnings("unchecked")
		public S codec(@Nonnull Codec<Buffer, IN, OUT> codec) {
			Assert.notNull(codec, "Codec cannot be null.");
			this.codec = codec;
			return (S) this;
		}

		/**
		 * Bypass any Reactor Buffer encoding for received data
		 *
		 * @param israw to enable raw data transfer from the server (e.g. ByteBuf from Netty).
		 * @return this
		 */
		@SuppressWarnings("unchecked")
		public S rawData(boolean israw) {
			if(israw){
				this.codec = NOOP_CODEC;
			}
			return (S) this;
		}
	}

	/**
	 * A helper class for specifying a {@code TcpClient}
	 *
	 * @param <IN>
	 * 		The type that will be received by the client
	 * @param <OUT>
	 * 		The type that will be sent by the client
	 *
	 * @author Jon Brisbin
	 * @author Stephane Maldini
	 */
	class TcpClientSpec<IN, OUT> extends DispatcherComponentSpec<TcpClientSpec<IN, OUT>, reactor.io.net.tcp.TcpClient<IN, OUT>> {

		private final Constructor<reactor.io.net.tcp.TcpClient> clientImplConstructor;

		private InetSocketAddress connectAddress;

		private ClientSocketOptions options = new ClientSocketOptions();

		private SslOptions sslOptions = null;
		private Codec<Buffer, IN, OUT> codec;

		/**
		 * Create a {@code TcpClient.Spec} using the given implementation class.
		 *
		 * @param clientImpl
		 * 		The concrete implementation of {@link reactor.io.net.tcp.TcpClient} to instantiate.
		 */
		@SuppressWarnings({"unchecked", "rawtypes"})
		TcpClientSpec(@Nonnull Class<? extends reactor.io.net.tcp.TcpClient> clientImpl) {
			Assert.notNull(clientImpl, "TcpClient implementation class cannot be null.");
			try {
				this.clientImplConstructor = (Constructor<reactor.io.net.tcp.TcpClient>) clientImpl.getDeclaredConstructor(
						Environment.class,
						Dispatcher.class,
						InetSocketAddress.class,
						ClientSocketOptions.class,
						SslOptions.class,
						Codec.class
				);
				this.clientImplConstructor.setAccessible(true);
			} catch (NoSuchMethodException e) {
				throw new IllegalArgumentException(
						"No public constructor found that matches the signature of the one found in the TcpClient class.");
			}
		}

		/**
		 * Set the common {@link reactor.io.net.config.ClientSocketOptions} for connections made in this client.
		 *
		 * @param options
		 * 		The socket options to apply to new connections.
		 *
		 * @return {@literal this}
		 */
		public TcpClientSpec<IN, OUT> options(ClientSocketOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * Set the options to use for configuring SSL. Setting this to {@code null} means don't use SSL at all (the
		 * default).
		 *
		 * @param sslOptions
		 * 		The options to set when configuring SSL
		 *
		 * @return {@literal this}
		 */
		public TcpClientSpec<IN, OUT> ssl(@Nullable SslOptions sslOptions) {
			this.sslOptions = sslOptions;
			return this;
		}

		/**
		 * The host and port to which this client should connect.
		 *
		 * @param host
		 * 		The host to connect to.
		 * @param port
		 * 		The port to connect to.
		 *
		 * @return {@literal this}
		 */
		public TcpClientSpec<IN, OUT> connect(@Nonnull String host, int port) {
			return connect(new InetSocketAddress(host, port));
		}

		/**
		 * The address to which this client should connect.
		 *
		 * @param connectAddress
		 * 		The address to connect to.
		 *
		 * @return {@literal this}
		 */
		public TcpClientSpec<IN, OUT> connect(@Nonnull InetSocketAddress connectAddress) {
			Assert.isNull(this.connectAddress, "Connect address is already set.");
			this.connectAddress = connectAddress;
			return this;
		}

		/**
		 * The {@link reactor.io.codec.Codec} to use to encode and decode data.
		 *
		 * @param codec
		 * 		The codec to use.
		 *
		 * @return {@literal this}
		 */
		public TcpClientSpec<IN, OUT> codec(@Nullable Codec<Buffer, IN, OUT> codec) {
			Assert.isNull(this.codec, "Codec has already been set.");
			this.codec = codec;
			return this;
		}

		/**
		 * Bypass any Reactor Buffer encoding for received data
		 *
		 * @param israw to enable raw data transfer from the server (e.g. ByteBuf from Netty).
		 * @return this
		 */
		@SuppressWarnings("unchecked")
		public TcpClientSpec<IN, OUT> rawData(boolean israw) {
			if(israw){
				this.codec = NOOP_CODEC;
			}
			return this;
		}

		@Override
		@SuppressWarnings("unchecked")
		protected reactor.io.net.tcp.TcpClient<IN, OUT> configure(Dispatcher dispatcher, Environment environment) {
			try {
				return clientImplConstructor.newInstance(
						environment,
						dispatcher,
						connectAddress,
						options,
						sslOptions,
						codec
				);
			} catch (Throwable t) {
				throw new IllegalStateException(t);
			}
		}

	}

	/**
	 * A TcpServerSpec is used to specify a TcpServer
	 *
	 * @param <IN>
	 * 		The type that will be received by this client
	 * @param <OUT>
	 * 		The type that will be sent by this client
	 *
	 * @author Jon Brisbin
	 * @author Stephane Maldini
	 */
	class TcpServerSpec<IN, OUT>
			extends ServerSpec<IN, OUT, ChannelStream<IN, OUT>, TcpServerSpec<IN, OUT>, reactor.io.net.tcp.TcpServer<IN, OUT>> {

		private final Constructor<? extends reactor.io.net.tcp.TcpServer> serverImplConstructor;

		private SslOptions sslOptions = null;

		/**
		 * Create a {@code TcpServer.Spec} using the given implementation class.
		 *
		 * @param serverImpl
		 * 		The concrete implementation of {@link reactor.io.net.tcp.TcpServer} to instantiate.
		 */
		@SuppressWarnings({"unchecked", "rawtypes"})
		TcpServerSpec(@Nonnull Class<? extends reactor.io.net.tcp.TcpServer> serverImpl) {
			Assert.notNull(serverImpl, "TcpServer implementation class cannot be null.");
			try {
				this.serverImplConstructor = serverImpl.getDeclaredConstructor(
						Environment.class,
						Dispatcher.class,
						InetSocketAddress.class,
						ServerSocketOptions.class,
						SslOptions.class,
						Codec.class
				);
				this.serverImplConstructor.setAccessible(true);
			} catch (NoSuchMethodException e) {
				throw new IllegalArgumentException(
						"No public constructor found that matches the signature of the one found in the TcpServer class.");
			}
		}

		/**
		 * Set the options to use for configuring SSL. Setting this to {@code null} means don't use SSL at all (the
		 * default).
		 *
		 * @param sslOptions
		 * 		The options to set when configuring SSL
		 *
		 * @return {@literal this}
		 */
		public TcpServerSpec<IN, OUT> ssl(@Nullable SslOptions sslOptions) {
			this.sslOptions = sslOptions;
			return this;
		}

		@SuppressWarnings("unchecked")
		@Override
		protected reactor.io.net.tcp.TcpServer<IN, OUT> configure(Dispatcher dispatcher, Environment env) {
			try {
				return serverImplConstructor.newInstance(
						env,
						dispatcher,
						listenAddress,
						options,
						sslOptions,
						codec
				);
			} catch (Throwable t) {
				throw new IllegalStateException(t);
			}
		}

	}



	/**
	 * @author Jon Brisbin
	 * @author Stephane Maldini
	 */
	class DatagramServerSpec<IN, OUT>
			extends ServerSpec<IN, OUT, ChannelStream<IN, OUT>, DatagramServerSpec<IN, OUT>, reactor.io.net.udp.DatagramServer<IN, OUT>> {
		protected final Constructor<? extends reactor.io.net.udp.DatagramServer> serverImplCtor;

		private NetworkInterface multicastInterface;

		DatagramServerSpec(Class<? extends reactor.io.net.udp.DatagramServer> serverImpl) {
			Assert.notNull(serverImpl, "NetServer implementation class cannot be null.");
			try {
				this.serverImplCtor = serverImpl.getDeclaredConstructor(
						Environment.class,
						Dispatcher.class,
						InetSocketAddress.class,
						NetworkInterface.class,
						ServerSocketOptions.class,
						Codec.class
				);
				this.serverImplCtor.setAccessible(true);
			} catch (NoSuchMethodException e) {
				throw new IllegalArgumentException(
						"No public constructor found that matches the signature of the one found in the DatagramServer class.");
			}
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
		protected reactor.io.net.udp.DatagramServer<IN, OUT> configure(Dispatcher dispatcher, Environment environment) {
			try {
				return serverImplCtor.newInstance(
						environment,
						dispatcher,
						listenAddress,
						multicastInterface,
						options,
						codec
				);
			} catch (Throwable t) {
				throw new IllegalStateException(t);
			}
		}

	}

	/**
	 * A HttpServer Spec is used to specify an HttpServer
	 *
	 * @param <IN>
	 * 		The type that will be received by this client
	 * @param <OUT>
	 * 		The type that will be sent by this client
	 *
	 * @author Jon Brisbin
	 * @author Stephane Maldini
	 */
	class HttpServerSpec<IN, OUT>
			extends ServerSpec<IN, OUT, HttpChannel<IN, OUT>, HttpServerSpec<IN, OUT>, reactor.io.net.http.HttpServer<IN, OUT>> {

		private final Constructor<? extends reactor.io.net.http.HttpServer> serverImplConstructor;

		private SslOptions sslOptions = null;

		/**
		 * Create a {@code TcpServer.Spec} using the given implementation class.
		 *
		 * @param serverImpl
		 * 		The concrete implementation of {@link reactor.io.net.http.HttpClient} to instantiate.
		 */
		@SuppressWarnings({"unchecked", "rawtypes"})
		HttpServerSpec(@Nonnull Class<? extends reactor.io.net.http.HttpServer> serverImpl) {
			Assert.notNull(serverImpl, "TcpServer implementation class cannot be null.");
			try {
				this.serverImplConstructor = serverImpl.getDeclaredConstructor(
						Environment.class,
						Dispatcher.class,
						InetSocketAddress.class,
						ServerSocketOptions.class,
						SslOptions.class,
						Codec.class
				);
				this.serverImplConstructor.setAccessible(true);
			} catch (NoSuchMethodException e) {
				throw new IllegalArgumentException(
						"No public constructor found that matches the signature of the one found in the TcpServer class.");
			}
		}

		/**
		 * Set the options to use for configuring SSL. Setting this to {@code null} means don't use SSL at all (the
		 * default).
		 *
		 * @param sslOptions
		 * 		The options to set when configuring SSL
		 *
		 * @return {@literal this}
		 */
		public HttpServerSpec<IN, OUT> ssl(@Nullable SslOptions sslOptions) {
			this.sslOptions = sslOptions;
			return this;
		}

		@SuppressWarnings("unchecked")
		@Override
		protected reactor.io.net.http.HttpServer<IN, OUT> configure(Dispatcher dispatcher, Environment env) {
			try {
				return serverImplConstructor.newInstance(
						env,
						dispatcher,
						listenAddress,
						options,
						sslOptions,
						codec
				);
			} catch (Throwable t) {
				throw new IllegalStateException(t);
			}
		}

	}

	/**
	 * A helper class for specifying a {@code HttpClient}
	 *
	 * @param <IN>
	 * 		The type that will be received by the client
	 * @param <OUT>
	 * 		The type that will be sent by the client
	 *
	 * @author Stephane Maldini
	 */
	class HttpClientSpec<IN, OUT> extends DispatcherComponentSpec<HttpClientSpec<IN, OUT>, reactor.io.net.http.HttpClient<IN, OUT>> {

		private final Constructor<reactor.io.net.http.HttpClient> clientImplConstructor;

		private InetSocketAddress connectAddress;
		private ClientSocketOptions options    = new ClientSocketOptions();
		private SslOptions          sslOptions = null;
		private Codec<Buffer, IN, OUT> codec;

		/**
		 * Create a {@code TcpClient.Spec} using the given implementation class.
		 *
		 * @param clientImpl
		 * 		The concrete implementation of {@link reactor.io.net.http.HttpClient} to instantiate.
		 */
		@SuppressWarnings({"unchecked", "rawtypes"})
		HttpClientSpec(@Nonnull Class<? extends reactor.io.net.http.HttpClient> clientImpl) {
			Assert.notNull(clientImpl, "TcpClient implementation class cannot be null.");
			try {
				this.clientImplConstructor = (Constructor<reactor.io.net.http.HttpClient>) clientImpl.getDeclaredConstructor(
						Environment.class,
						Dispatcher.class,
						InetSocketAddress.class,
						ClientSocketOptions.class,
						SslOptions.class,
						Codec.class
				);
				this.clientImplConstructor.setAccessible(true);
			} catch (NoSuchMethodException e) {
				throw new IllegalArgumentException(
						"No public constructor found that matches the signature of the one found in the TcpClient class.");
			}
		}

		/**
		 * Set the common {@link reactor.io.net.config.ClientSocketOptions} for connections made in this client.
		 *
		 * @param options
		 * 		The socket options to apply to new connections.
		 *
		 * @return {@literal this}
		 */
		public HttpClientSpec<IN, OUT> options(ClientSocketOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * Set the options to use for configuring SSL. Setting this to {@code null} means don't use SSL at all (the
		 * default).
		 *
		 * @param sslOptions
		 * 		The options to set when configuring SSL
		 *
		 * @return {@literal this}
		 */
		public HttpClientSpec<IN, OUT> ssl(@Nullable SslOptions sslOptions) {
			this.sslOptions = sslOptions;
			return this;
		}

		/**
		 * The host and port to which this client should connect.
		 *
		 * @param host
		 * 		The host to connect to.
		 * @param port
		 * 		The port to connect to.
		 *
		 * @return {@literal this}
		 */
		public HttpClientSpec<IN, OUT> connect(@Nonnull String host, int port) {
			return connect(new InetSocketAddress(host, port));
		}

		/**
		 * The address to which this client should connect.
		 *
		 * @param connectAddress
		 * 		The address to connect to.
		 *
		 * @return {@literal this}
		 */
		public HttpClientSpec<IN, OUT> connect(@Nonnull InetSocketAddress connectAddress) {
			Assert.isNull(this.connectAddress, "Connect address is already set.");
			this.connectAddress = connectAddress;
			return this;
		}

		/**
		 * The {@link reactor.io.codec.Codec} to use to encode and decode data.
		 *
		 * @param codec
		 * 		The codec to use.
		 *
		 * @return {@literal this}
		 */
		public HttpClientSpec<IN, OUT> codec(@Nullable Codec<Buffer, IN, OUT> codec) {
			Assert.isNull(this.codec, "Codec has already been set.");
			this.codec = codec;
			return this;
		}

		@Override
		@SuppressWarnings("unchecked")
		protected reactor.io.net.http.HttpClient<IN, OUT> configure(Dispatcher dispatcher, Environment environment) {
			try {
				return clientImplConstructor.newInstance(
						environment,
						dispatcher,
						connectAddress,
						options,
						sslOptions,
						codec
				);
			} catch (Throwable t) {
				throw new IllegalStateException(t);
			}
		}

	}

	/**
	 * A helper class for configure a new {@code Reconnect}.
	 *
	 */
	class IncrementalBackoffReconnect implements Supplier<Reconnect> {

		public static final long DEFAULT_INTERVAL = 5000;

		public static final long DEFAULT_MULTIPLIER   = 1;
		public static final long DEFAULT_MAX_ATTEMPTS = -1;
		private final List<InetSocketAddress> addresses;
		private       long                    interval;

		private long multiplier;
		private long maxInterval;
		private long maxAttempts;

		/**
		 *
		 */
		IncrementalBackoffReconnect() {
			this.addresses = new LinkedList<InetSocketAddress>();
			this.interval = DEFAULT_INTERVAL;
			this.multiplier = DEFAULT_MULTIPLIER;
			this.maxInterval = Long.MAX_VALUE;
			this.maxAttempts = DEFAULT_MAX_ATTEMPTS;
		}

		/**
		 * Set the reconnection interval.
		 *
		 * @param interval the period reactor waits between attemps to reconnect disconnected peers
		 * @return {@literal this}
		 */
		public IncrementalBackoffReconnect interval(long interval) {
			this.interval = interval;
			return this;
		}

		/**
		 * Set the maximum reconnection interval that will be applied if the multiplier
		 * is set to a value greather than one.
		 *
		 * @param maxInterval
		 * @return {@literal this}
		 */
		public IncrementalBackoffReconnect maxInterval(long maxInterval) {
			this.maxInterval = maxInterval;
			return this;
		}

		/**
		 * Set the backoff multiplier.
		 *
		 * @param multiplier
		 * @return {@literal this}
		 */
		public IncrementalBackoffReconnect multiplier(long multiplier) {
			this.multiplier = multiplier;
			return this;
		}

		/**
		 * Sets the number of time that Reactor will attempt to connect or reconnect
		 * before giving up.
		 *
		 * @param maxAttempts The max number of attempts made before failing.
		 * @return {@literal this}
		 */
		public IncrementalBackoffReconnect maxAttempts(long maxAttempts) {
			this.maxAttempts = maxAttempts;
			return this;
		}

		/**
		 * Add an address to the pool of addresses.
		 *
		 * @param address
		 * @return {@literal this}
		 */
		public IncrementalBackoffReconnect address(InetSocketAddress address) {
			this.addresses.add(address);
			return this;
		}

		/**
		 * Add an address to the pool of addresses.
		 *
		 * @param host
		 * @param port
		 * @return {@literal this}
		 */
		public IncrementalBackoffReconnect address(String host, int port) {
			this.addresses.add(new InetSocketAddress(host, port));
			return this;
		}

		@Override
		public Reconnect get() {
			final Supplier<InetSocketAddress> endpoints =
					Suppliers.roundRobin(addresses.toArray(new InetSocketAddress[]{}));

			return new Reconnect() {
				public Tuple2<InetSocketAddress, Long> reconnect(InetSocketAddress currentAddress, int attempt) {
					Tuple2<InetSocketAddress, Long> rv = null;
					synchronized (IncrementalBackoffReconnect.this) {
						if (!addresses.isEmpty()) {
							if (IncrementalBackoffReconnect.this.maxAttempts == -1 ||
									IncrementalBackoffReconnect.this.maxAttempts > attempt) {
								rv = Tuple.of(endpoints.get(), determineInterval(attempt));
							}
						} else {
							rv = Tuple.of(currentAddress, determineInterval(attempt));
						}
					}

					return rv;
				}
			};
		}

		/**
		 * Determine the period in milliseconds between reconnection attempts.
		 *
		 * @param attempt the number of times a reconnection has been attempted
		 * @return the reconnection period
		 */
		public long determineInterval(int attempt) {
			return (multiplier > 1) ? Math.min(maxInterval, interval * attempt) : interval;
		}

	}
}
