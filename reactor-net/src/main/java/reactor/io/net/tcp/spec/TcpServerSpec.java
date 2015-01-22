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
import reactor.core.Dispatcher;
import reactor.core.support.Assert;
import reactor.io.codec.Codec;
import reactor.io.net.NetChannelStream;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.spec.NetServerSpec;
import reactor.io.net.tcp.TcpServer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;

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
public class TcpServerSpec<IN, OUT>
		extends NetServerSpec<IN, OUT, NetChannelStream<IN, OUT>, TcpServerSpec<IN, OUT>, TcpServer<IN, OUT>> {

	private final Constructor<? extends TcpServer> serverImplConstructor;

	private SslOptions sslOptions = null;

	/**
	 * Create a {@code TcpServer.Spec} using the given implementation class.
	 *
	 * @param serverImpl
	 * 		The concrete implementation of {@link TcpServer} to instantiate.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public TcpServerSpec(@Nonnull Class<? extends TcpServer> serverImpl) {
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
	 * Set the options to use for configuring SSL. Setting this to {@code null} means don't use SSL at all (the default).
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
	protected TcpServer<IN, OUT> configure(Dispatcher dispatcher, Environment env) {
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
