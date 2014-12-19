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

package reactor.net.tcp;

import reactor.Environment;
import reactor.bus.EventBus;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.net.AbstractNetPeer;
import reactor.net.NetChannel;
import reactor.net.NetClient;
import reactor.net.Reconnect;
import reactor.net.config.ClientSocketOptions;
import reactor.net.config.SslOptions;
import reactor.rx.Promise;
import reactor.rx.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Collection;

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
public abstract class TcpClient<IN, OUT>
		extends AbstractNetPeer<IN, OUT>
		implements NetClient<IN, OUT> {

	private final InetSocketAddress   connectAddress;
	private final ClientSocketOptions options;
	private final SslOptions          sslOptions;

	protected TcpClient(@Nonnull Environment env,
	                    @Nonnull EventBus reactor,
	                    @Nullable InetSocketAddress connectAddress,
	                    @Nullable ClientSocketOptions options,
	                    @Nullable SslOptions sslOptions,
	                    @Nullable Codec<Buffer, IN, OUT> codec,
	                    @Nonnull Collection<Consumer<NetChannel<IN, OUT>>> consumers) {
		super(env, reactor, codec, consumers);
		this.connectAddress = (null != connectAddress ? connectAddress : new InetSocketAddress("127.0.0.1", 3000));
		this.options = options;
		this.sslOptions = sslOptions;
	}

	/**
	 * Open a {@link NetChannel} to the configured host:port and return a {@link reactor.rx.Promise} that
	 * will be fulfilled when the client is connected.
	 *
	 * @return A {@link reactor.rx.Promise} that will be filled with the {@link NetChannel} when
	 * connected.
	 */
	public abstract Promise<NetChannel<IN, OUT>> open();

	/**
	 * Open a {@link NetChannel} to the configured host:port and return a {@link Stream} that will be passed a new {@link
	 * NetChannel} object every time the client is connected to the endpoint. The given {@link reactor.net.Reconnect}
	 * describes how the client should attempt to reconnect to the host if the initial connection fails or if the client
	 * successfully connects but at some point in the future gets cut off from the host. The {@code Reconnect} tells the
	 * client where to try reconnecting and gives a delay describing how long to wait to attempt to reconnect. When the
	 * connect is successfully made, the {@link Stream} is sent a new {@link NetChannel} backed by the newly-connected
	 * connection.
	 *
	 * @param reconnect
	 *
	 * @return
	 */
	public abstract Stream<NetChannel<IN, OUT>> open(Reconnect reconnect);

	/**
	 * Get the {@link java.net.InetSocketAddress} to which this client must connect.
	 *
	 * @return the connect address
	 */
	public InetSocketAddress getConnectAddress() {
		return connectAddress;
	}

	/**
	 * Get the {@link reactor.net.config.ClientSocketOptions} currently in effect.
	 *
	 * @return the client options
	 */
	protected ClientSocketOptions getOptions() {
		return this.options;
	}

	/**
	 * Get the {@link reactor.net.config.SslOptions} current in effect.
	 *
	 * @return the SSL options
	 */
	protected SslOptions getSslOptions() {
		return sslOptions;
	}

}
