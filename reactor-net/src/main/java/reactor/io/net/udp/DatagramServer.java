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

package reactor.io.net.udp;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;

import org.reactivestreams.Publisher;
import reactor.Processors;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.io.net.Preprocessor;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactivePeer;
import reactor.io.net.config.ServerSocketOptions;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class DatagramServer<IN, OUT>
		extends ReactivePeer<IN, OUT, ReactiveChannel<IN, OUT>> {

	public static final int DEFAULT_UDP_THREAD_COUNT = Integer.parseInt(
	  System.getProperty("reactor.udp.ioThreadCount",
		"" + Processors.DEFAULT_POOL_SIZE)
	);

	private final InetSocketAddress   listenAddress;
	private final NetworkInterface    multicastInterface;
	private final ServerSocketOptions options;

	protected DatagramServer(Timer timer,
	                         InetSocketAddress listenAddress,
	                         NetworkInterface multicastInterface,
	                         ServerSocketOptions options) {
		super(timer, options != null ? options.prefetch() : Long.MAX_VALUE);
		this.listenAddress = listenAddress;
		this.multicastInterface = multicastInterface;
		this.options = options;
	}

	/**
	 * Join a multicast group.
	 *
	 * @param multicastAddress multicast address of the group to join
	 * @param iface            interface to use for multicast
	 * @return a {@link Publisher} that will be complete when the group has been joined
	 */
	public abstract Publisher<Void> join(InetAddress multicastAddress, NetworkInterface iface);

	/**
	 * Join a multicast group.
	 *
	 * @param multicastAddress multicast address of the group to join
	 * @return a {@link Publisher} that will be complete when the group has been joined
	 */
	public Publisher<Void> join(InetAddress multicastAddress) {
		return join(multicastAddress, null);
	}

	/**
	 * Leave a multicast group.
	 *
	 * @param multicastAddress multicast address of the group to leave
	 * @param iface            interface to use for multicast
	 * @return a {@link Publisher} that will be complete when the group has been left
	 */
	public abstract Publisher<Void> leave(InetAddress multicastAddress, NetworkInterface iface);

	/**
	 * Leave a multicast group.
	 *
	 * @param multicastAddress multicast address of the group to leave
	 * @return a {@link Publisher} that will be complete when the group has been left
	 */
	public Publisher<Void> leave(InetAddress multicastAddress) {
		return leave(multicastAddress, null);
	}

	/**
	 * Get the address to which this server is bound.
	 *
	 * @return the bind address
	 */
	public InetSocketAddress getListenAddress() {
		return listenAddress;
	}

	/**
	 * Get the {@link java.net.NetworkInterface} on which multicast will be performed.
	 *
	 * @return the multicast NetworkInterface
	 */
	public NetworkInterface getMulticastInterface() {
		return multicastInterface;
	}

	/**
	 * Get the {@link reactor.io.net.config.ServerSocketOptions} currently in effect.
	 *
	 * @return the server options in use
	 */
	protected ServerSocketOptions getOptions() {
		return options;
	}

	@Override
	protected <NEWIN, NEWOUT> ReactivePeer<NEWIN, NEWOUT, ReactiveChannel<NEWIN, NEWOUT>> doPreprocessor(
			Function<? super ReactiveChannel<IN, OUT>, ? extends ReactiveChannel<NEWIN, NEWOUT>> preprocessor) {
		return new PreprocessedDatagramServer<>(preprocessor);
	}

	private final class PreprocessedDatagramServer<NEWIN, NEWOUT, NEWCONN extends ReactiveChannel<NEWIN, NEWOUT>>
			extends DatagramServer<NEWIN, NEWOUT> {

		private final Function<? super ReactiveChannel<IN, OUT>, ? extends NEWCONN>
				preprocessor;

		public PreprocessedDatagramServer(Function<? super ReactiveChannel<IN, OUT>, ? extends NEWCONN> preprocessor) {
			super(DatagramServer.this.getDefaultTimer(), DatagramServer.this.getListenAddress(), DatagramServer.this.getMulticastInterface(), DatagramServer.this.getOptions());
			this.preprocessor = preprocessor;
		}

		@Override
		protected Publisher<Void> doStart(
				ReactiveChannelHandler<NEWIN, NEWOUT, ReactiveChannel<NEWIN, NEWOUT>> handler) {
			ReactiveChannelHandler<IN, OUT, ReactiveChannel<IN, OUT>> p = Preprocessor.PreprocessedHandler.create(handler, preprocessor);
			return DatagramServer.this.start(p);
		}

		@Override
		public Publisher<Void> join(InetAddress multicastAddress,
				NetworkInterface iface) {
			return DatagramServer.this.join(multicastAddress, iface);
		}

		@Override
		public Publisher<Void> leave(InetAddress multicastAddress,
				NetworkInterface iface) {
			return DatagramServer.this.leave(multicastAddress, iface);
		}

		@Override
		protected Publisher<Void> doShutdown() {
			return DatagramServer.this.shutdown();
		}

		@Override
		protected boolean shouldFailOnStarted() {
			return false;
		}
	}
}
