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

import org.reactivestreams.Publisher;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.support.Assert;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.ChannelStream;
import reactor.io.net.PeerStream;
import reactor.io.net.Server;
import reactor.io.net.config.ServerSocketOptions;
import reactor.rx.Promise;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class DatagramServer<IN, OUT>
		extends PeerStream<IN, OUT, ChannelStream<IN, OUT>>
		implements Server<IN, OUT, ChannelStream<IN, OUT>> {

	private final InetSocketAddress   listenAddress;
	private final NetworkInterface    multicastInterface;
	private final ServerSocketOptions options;

	protected DatagramServer(@Nonnull Environment env,
	                         @Nonnull Dispatcher dispatcher,
	                         @Nullable InetSocketAddress listenAddress,
	                         @Nullable NetworkInterface multicastInterface,
	                         @Nonnull ServerSocketOptions options,
	                         @Nullable Codec<Buffer, IN, OUT> codec) {
		super(env, dispatcher, codec);
		Assert.notNull(options, "ServerSocketOptions cannot be null");
		this.listenAddress = listenAddress;
		this.multicastInterface = multicastInterface;
		this.options = options;
	}

	@Override
	public Server<IN, OUT, ChannelStream<IN, OUT>> pipeline(
			final Function<ChannelStream<IN, OUT>, ? extends Publisher<? extends OUT>> serviceFunction) {
		doPipeline(serviceFunction);
		return this;
	}

	/**
	 * Start this server.
	 *
	 * @return {@literal this}
	 */
	public abstract Promise<Boolean> start();


	/**
	 * Join a multicast group.
	 *
	 * @param multicastAddress
	 * 		multicast address of the group to join
	 * @param iface
	 * 		interface to use for multicast
	 *
	 * @return {@literal this}
	 */
	public abstract Promise<Void> join(InetAddress multicastAddress, NetworkInterface iface);

	/**
	 * Join a multicast group.
	 *
	 * @param multicastAddress
	 * 		multicast address of the group to join
	 *
	 * @return {@literal this}
	 */
	public Promise<Void> join(InetAddress multicastAddress) {
		return join(multicastAddress, null);
	}

	/**
	 * Leave a multicast group.
	 *
	 * @param multicastAddress
	 * 		multicast address of the group to leave
	 * @param iface
	 * 		interface to use for multicast
	 *
	 * @return {@literal this}
	 */
	public abstract Promise<Void> leave(InetAddress multicastAddress, NetworkInterface iface);

	/**
	 * Leave a multicast group.
	 *
	 * @param multicastAddress
	 * 		multicast address of the group to leave
	 *
	 * @return {@literal this}
	 */
	public Promise<Void> leave(InetAddress multicastAddress) {
		return leave(multicastAddress, null);
	}

	/**
	 * Get the address to which this server is bound.
	 *
	 * @return
	 */
	protected InetSocketAddress getListenAddress() {
		return listenAddress;
	}

	/**
	 * Get the {@link java.net.NetworkInterface} on which multicast will be performed.
	 *
	 * @return
	 */
	protected NetworkInterface getMulticastInterface() { return multicastInterface; }

	/**
	 * Get the {@link reactor.io.net.config.ServerSocketOptions} currently in effect.
	 *
	 * @return
	 */
	protected ServerSocketOptions getOptions() {
		return options;
	}

}
