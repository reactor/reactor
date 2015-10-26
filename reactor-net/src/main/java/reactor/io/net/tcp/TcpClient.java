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
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.fn.tuple.Tuple2;
import reactor.io.net.Preprocessor;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactiveClient;
import reactor.io.net.ReactivePeer;
import reactor.io.net.Reconnect;
import reactor.io.net.config.ClientSocketOptions;
import reactor.io.net.config.SslOptions;

/**
 * The base class for a Reactor-based TCP client.
 *
 * @param <IN>  The type that will be received by this client
 * @param <OUT> The type that will be sent by this client
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class TcpClient<IN, OUT>
		extends ReactiveClient<IN, OUT, ReactiveChannel<IN, OUT>> {

	private final InetSocketAddress   connectAddress;
	private final ClientSocketOptions options;
	private final SslOptions          sslOptions;

	protected TcpClient(Timer timer,
	                    InetSocketAddress connectAddress,
	                    ClientSocketOptions options,
	                    SslOptions sslOptions) {
		super(timer, options != null ? options.prefetch() : Long.MAX_VALUE);
		this.connectAddress = (null != connectAddress ? connectAddress : new InetSocketAddress("127.0.0.1", 3000));
		this.options = options;
		this.sslOptions = sslOptions;
	}

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

	@Override
	protected <NEWIN, NEWOUT> ReactivePeer<NEWIN, NEWOUT, ReactiveChannel<NEWIN, NEWOUT>> doPreprocessor(
			Function<? super ReactiveChannel<IN, OUT>, ? extends ReactiveChannel<NEWIN, NEWOUT>> preprocessor) {
		return new PreprocessedTcpClient<>(preprocessor);
	}

	private final class PreprocessedTcpClient<NEWIN, NEWOUT, NEWCONN extends ReactiveChannel<NEWIN, NEWOUT>>
			extends TcpClient<NEWIN, NEWOUT> {

		private final Function<? super ReactiveChannel<IN, OUT>, ? extends NEWCONN>
				preprocessor;

		public PreprocessedTcpClient(Function<? super ReactiveChannel<IN, OUT>, ? extends NEWCONN> preprocessor) {
			super(TcpClient.this.getDefaultTimer(), TcpClient.this.getConnectAddress(), TcpClient.this.getOptions(), TcpClient.this.getSslOptions());
			this.preprocessor = preprocessor;
		}

		@Override
		protected Publisher<Void> doStart(
				ReactiveChannelHandler<NEWIN, NEWOUT, ReactiveChannel<NEWIN, NEWOUT>> handler) {
			ReactiveChannelHandler<IN, OUT, ReactiveChannel<IN, OUT>> p = Preprocessor.PreprocessedHandler.create(handler, preprocessor);
			return TcpClient.this.start(p);
		}

		@Override
		protected Publisher<Tuple2<InetSocketAddress, Integer>> doStart(
				ReactiveChannelHandler<NEWIN, NEWOUT, ReactiveChannel<NEWIN, NEWOUT>> handler,
				Reconnect reconnect) {
			ReactiveChannelHandler<IN, OUT, ReactiveChannel<IN, OUT>> p = Preprocessor.PreprocessedHandler.create(handler, preprocessor);
			return TcpClient.this.start(p, reconnect);
		}

		@Override
		protected Publisher<Void> doShutdown() {
			return TcpClient.this.shutdown();
		}

		@Override
		protected boolean shouldFailOnStarted() {
			return false;
		}
	}
}
