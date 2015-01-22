/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.io.net.tcp.spec;

import reactor.Environment;
import reactor.bus.spec.DispatcherComponentSpec;
import reactor.core.Dispatcher;
import reactor.core.support.Assert;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.config.ClientSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.tcp.TcpClient;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;

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
public class TcpClientSpec<IN, OUT> extends DispatcherComponentSpec<TcpClientSpec<IN, OUT>, TcpClient<IN, OUT>> {

	private final Constructor<TcpClient<IN, OUT>> clientImplConstructor;

	private InetSocketAddress connectAddress;
	private ClientSocketOptions      options    = new ClientSocketOptions();
	private SslOptions               sslOptions = null;
	private Codec<Buffer, IN, OUT> codec;

	/**
	 * Create a {@code TcpClient.Spec} using the given implementation class.
	 *
	 * @param clientImpl
	 * 		The concrete implementation of {@link TcpClient} to instantiate.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public TcpClientSpec(@Nonnull Class<? extends TcpClient> clientImpl) {
		Assert.notNull(clientImpl, "TcpClient implementation class cannot be null.");
		try {
			this.clientImplConstructor = (Constructor<TcpClient<IN, OUT>>) clientImpl.getDeclaredConstructor(
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
	 * Set the common {@link ClientSocketOptions} for connections made in this client.
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
	 * Set the options to use for configuring SSL. Setting this to {@code null} means don't use SSL at all (the default).
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
	 * The {@link Codec} to use to encode and decode data.
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

	@Override
	protected TcpClient<IN, OUT> configure(Dispatcher dispatcher, Environment environment) {
		try {
			return clientImplConstructor.newInstance(
					environment,
					dispatcher,
					connectAddress,
					options,
					sslOptions,
					codec
			);
		} catch(Throwable t) {
			throw new IllegalStateException(t);
		}
	}

}
