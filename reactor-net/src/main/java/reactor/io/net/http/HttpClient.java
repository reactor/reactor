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

package reactor.io.net.http;

import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.NetChannelStream;
import reactor.io.net.NetClient;
import reactor.io.net.NetPeerStream;
import reactor.io.net.Reconnect;
import reactor.io.net.config.ClientSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.rx.Promise;
import reactor.rx.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;

/**
 * The base class for a Reactor-based TCP client.
 *
 * @param <IN>
 * 		The type that will be received by this client
 * @param <OUT>
 * 		The type that will be sent by this client
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class HttpClient<IN, OUT>
		extends NetPeerStream<IN, OUT>
		implements NetClient<IN, OUT, NetChannelStream<IN, OUT>> {

	private final InetSocketAddress   connectAddress;
	private final ClientSocketOptions options;
	private final SslOptions          sslOptions;

	protected HttpClient(@Nonnull Environment env,
	                     @Nonnull Dispatcher dispatcher,
	                     @Nullable InetSocketAddress connectAddress,
	                     @Nullable ClientSocketOptions options,
	                     @Nullable SslOptions sslOptions,
	                     @Nullable Codec<Buffer, IN, OUT> codec) {
		super(env, dispatcher, codec);
		this.connectAddress = (null != connectAddress ? connectAddress : new InetSocketAddress("127.0.0.1", 3000));
		this.options = options;
		this.sslOptions = sslOptions;
	}

	/**
	 * Open a {@link reactor.io.net.NetChannelStream} to the configured host:port and return a {@link reactor.rx.Promise} that
	 * will be fulfilled when the client is connected.
	 *
	 * @return A {@link reactor.rx.Promise} that will be filled with the {@link reactor.io.net.NetChannelStream} when
	 * connected.
	 */
	public abstract Promise<NetChannelStream<IN, OUT>> open();

	/**
	 * Open a {@link reactor.io.net.NetChannelStream} to the configured host:port and return a {@link reactor.rx.Stream} that will be passed a new {@link
	 * reactor.io.net.NetChannel} object every time the client is connected to the endpoint. The given {@link reactor.io.net.Reconnect}
	 * describes how the client should attempt to reconnect to the host if the initial connection fails or if the client
	 * successfully connects but at some point in the future gets cut off from the host. The {@code Reconnect} tells the
	 * client where to try reconnecting and gives a delay describing how long to wait to attempt to reconnect. When the
	 * connect is successfully made, the {@link reactor.rx.Stream} is sent a new {@link reactor.io.net.NetChannelStream} backed by the newly-connected
	 * connection.
	 *
	 * @param reconnect
	 *
	 * @return
	 */
	public abstract Stream<NetChannelStream<IN, OUT>> open(Reconnect reconnect);

	/**
	 * Get the {@link java.net.InetSocketAddress} to which this client must connect.
	 *
	 * @return the connect address
	 */
	public InetSocketAddress getConnectAddress() {
		return connectAddress;
	}

	/**
	 * Get the {@link reactor.io.net.config.ClientSocketOptions} currently in effect.
	 *
	 * @return the client options
	 */
	protected ClientSocketOptions getOptions() {
		return this.options;
	}

	/**
	 * Get the {@link reactor.io.net.config.SslOptions} current in effect.
	 *
	 * @return the SSL options
	 */
	protected SslOptions getSslOptions() {
		return sslOptions;
	}

}
