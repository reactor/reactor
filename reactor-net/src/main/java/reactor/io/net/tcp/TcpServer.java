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

import java.net.InetSocketAddress;

import org.reactivestreams.Publisher;
import reactor.Processors;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.io.net.Preprocessor;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactivePeer;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.config.SslOptions;

/**
 * Base functionality needed by all servers that communicate with clients over TCP.
 *
 * @param <IN>  The type that will be received by this server
 * @param <OUT> The type that will be sent by this server
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class TcpServer<IN, OUT>
		extends ReactivePeer<IN, OUT, ReactiveChannel<IN, OUT>> {

	public static final int DEFAULT_TCP_THREAD_COUNT = Integer.parseInt(
	  System.getProperty("reactor.tcp.selectThreadCount",
		""+Processors.DEFAULT_POOL_SIZE / 2)
	);

	public static final int DEFAULT_TCP_SELECT_COUNT = Integer.parseInt(
	  System.getProperty("reactor.tcp.selectThreadCount",
		""+DEFAULT_TCP_THREAD_COUNT)
	);

	private final ServerSocketOptions options;
	private final SslOptions          sslOptions;


	//Carefully reset
	protected InetSocketAddress listenAddress;

	protected TcpServer(Timer timer,
	                    InetSocketAddress listenAddress,
	                    ServerSocketOptions options,
	                    SslOptions sslOptions) {
		super(timer, options != null ? options.prefetch() : Long.MAX_VALUE);
		this.listenAddress = listenAddress;
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

	@Override
	protected <NEWIN, NEWOUT> ReactivePeer<NEWIN, NEWOUT, ReactiveChannel<NEWIN, NEWOUT>> doPreprocessor(
			Function<? super ReactiveChannel<IN, OUT>, ? extends ReactiveChannel<NEWIN, NEWOUT>> preprocessor) {
		return new PreprocessedTcpServer<>(preprocessor);
	}

	private final class PreprocessedTcpServer<NEWIN, NEWOUT, NEWCONN extends ReactiveChannel<NEWIN, NEWOUT>>
			extends TcpServer<NEWIN, NEWOUT> {

		private final Function<? super ReactiveChannel<IN, OUT>, ? extends NEWCONN> preprocessor;

		public PreprocessedTcpServer(Function<? super ReactiveChannel<IN, OUT>, ? extends NEWCONN> preprocessor) {
			super(TcpServer.this.getDefaultTimer(), TcpServer.this.getListenAddress(), TcpServer.this.getOptions(), TcpServer.this.getSslOptions());
			this.preprocessor = preprocessor;
		}

		@Override
		protected Publisher<Void> doStart(
				ReactiveChannelHandler<NEWIN, NEWOUT, ReactiveChannel<NEWIN, NEWOUT>> handler) {
			ReactiveChannelHandler<IN, OUT, ReactiveChannel<IN, OUT>> p = Preprocessor.PreprocessedHandler.create(handler, preprocessor);
			return TcpServer.this.start(p);
		}

		@Override
		protected Publisher<Void> doShutdown() {
			return TcpServer.this.shutdown();
		}

		@Override
		public InetSocketAddress getListenAddress() {
			return TcpServer.this.getListenAddress();
		}

		@Override
		protected boolean shouldFailOnStarted() {
			return false;
		}
	}
}
