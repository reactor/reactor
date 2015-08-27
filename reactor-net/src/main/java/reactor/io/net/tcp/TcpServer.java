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

package reactor.io.net.tcp;

import reactor.core.support.Assert;
import reactor.fn.timer.Timer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.ChannelStream;
import reactor.io.net.ReactorPeer;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.config.SslOptions;

import java.net.InetSocketAddress;

/**
 * Base functionality needed by all servers that communicate with clients over TCP.
 *
 * @param <IN>  The type that will be received by this server
 * @param <OUT> The type that will be sent by this server
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class TcpServer<IN, OUT>
  extends ReactorPeer<IN, OUT, ChannelStream<IN, OUT>> {

	private final ServerSocketOptions options;
	private final SslOptions          sslOptions;


	//Carefully reset
	protected InetSocketAddress listenAddress;

	protected TcpServer(Timer timer,
	                    InetSocketAddress listenAddress,
	                    ServerSocketOptions options,
	                    SslOptions sslOptions,
	                    Codec<Buffer, IN, OUT> codec) {
		super(timer, codec, options.prefetch());
		this.listenAddress = listenAddress;
		Assert.notNull(options, "ServerSocketOptions cannot be null");
		this.options = options;
		this.sslOptions = sslOptions;
	}

	/**
	 * Get the address to which this server is bound. If port 0 was used, returns the resolved port if possible
	 *
	 * @return the address bound
	 */
	public InetSocketAddress getListenAddress() {
		return listenAddress;
	}

	/**
	 * Get the {@link reactor.io.net.config.ServerSocketOptions} currently in effect.
	 *
	 * @return the current server options
	 */
	protected ServerSocketOptions getOptions() {
		return options;
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
