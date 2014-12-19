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
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.net.AbstractNetPeer;
import reactor.net.NetChannel;
import reactor.net.NetServer;
import reactor.net.config.ServerSocketOptions;
import reactor.net.config.SslOptions;
import reactor.rx.Promise;
import reactor.rx.Promises;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * Base functionality needed by all servers that communicate with clients over TCP.
 *
 * @param <IN>
 * 		The type that will be received by this server
 * @param <OUT>
 * 		The type that will be sent by this server
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class TcpServer<IN, OUT>
		extends AbstractNetPeer<IN, OUT>
		implements NetServer<IN, OUT> {

	private final InetSocketAddress   listenAddress;
	private final ServerSocketOptions options;
	private final SslOptions          sslOptions;

	protected TcpServer(@Nonnull Environment env,
	                    @Nonnull EventBus reactor,
	                    @Nullable InetSocketAddress listenAddress,
	                    ServerSocketOptions options,
	                    SslOptions sslOptions,
	                    @Nullable Codec<Buffer, IN, OUT> codec,
	                    @Nonnull Collection<Consumer<NetChannel<IN, OUT>>> consumers) {
		super(env, reactor, codec, consumers);
		this.listenAddress = listenAddress;
		Assert.notNull(options, "ServerSocketOptions cannot be null");
		this.options = options;
		this.sslOptions = sslOptions;
	}

	/**
	 * Start this server.
	 *
	 * @return {@literal this}
	 */
	public Promise<Boolean> start() {
		final Promise<Boolean> d = Promises.ready(getEnvironment(), getReactor().getDispatcher());
		start(new Runnable() {
			@Override
			public void run() {
				d.onNext(true);
			}
		});
		return d;
	}

	/**
	 * Start this server, invoking the given callback when the server has started.
	 *
	 * @param started
	 * 		Callback to invoke when the server is started. May be {@literal null}.
	 *
	 * @return {@literal this}
	 */
	public abstract TcpServer<IN, OUT> start(@Nullable Runnable started);

	/**
	 * Get the address to which this server is bound.
	 *
	 * @return
	 */
	protected InetSocketAddress getListenAddress() {
		return listenAddress;
	}

	/**
	 * Get the {@link reactor.net.config.ServerSocketOptions} currently in effect.
	 *
	 * @return
	 */
	protected ServerSocketOptions getOptions() {
		return options;
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
