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

package reactor.net.spec;

import reactor.bus.spec.EventRoutingComponentSpec;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.net.NetChannel;
import reactor.net.NetServer;
import reactor.net.config.ServerSocketOptions;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;

/**
 * @author Jon Brisbin
 */
public abstract class NetServerSpec<IN, OUT, S extends NetServerSpec<IN, OUT, S, N>, N extends NetServer<IN, OUT>>
		extends EventRoutingComponentSpec<S, N> {

	protected ServerSocketOptions                       options          = new ServerSocketOptions();
	protected Collection<Consumer<NetChannel<IN, OUT>>> channelConsumers = Collections.emptyList();
	protected InetSocketAddress      listenAddress;
	protected Codec<Buffer, IN, OUT> codec;

	/**
	 * Set the common {@link ServerSocketOptions} for channels made in this server.
	 *
	 * @param options
	 * 		The options to set when new channels are made.
	 *
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public S options(@Nonnull ServerSocketOptions options) {
		Assert.notNull(options, "ServerSocketOptions cannot be null.");
		this.options = options;
		return (S) this;
	}

	/**
	 * The port on which this server should listen, assuming it should bind to all available addresses.
	 *
	 * @param port
	 * 		The port to listen on.
	 *
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public S listen(int port) {
		return listen(new InetSocketAddress(port));
	}

	/**
	 * The host and port on which this server should listen.
	 *
	 * @param host
	 * 		The host to bind to.
	 * @param port
	 * 		The port to listen on.
	 *
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public S listen(String host, int port) {
		if (null == host) {
			host = "localhost";
		}
		return listen(new InetSocketAddress(host, port));
	}

	/**
	 * The {@link java.net.InetSocketAddress} on which this server should listen.
	 *
	 * @param listenAddress
	 * 		the listen address
	 *
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public S listen(InetSocketAddress listenAddress) {
		this.listenAddress = listenAddress;
		return (S)this;
	}

	/**
	 * The {@link Codec} to use to encode and decode data.
	 *
	 * @param codec
	 * 		The codec to use.
	 *
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public S codec(@Nonnull Codec<Buffer, IN, OUT> codec) {
		Assert.notNull(codec, "Codec cannot be null.");
		this.codec = codec;
		return (S)this;
	}

	/**
	 * Callback to invoke when a new input message is created.
	 *
	 * @param inputConsumer
	 * 		The callback to invoke for new messages.
	 *
	 * @return {@literal this}
	 */
	public S consumeInput(@Nonnull final Consumer<IN> inputConsumer) {
		Collection<Consumer<NetChannel<IN, OUT>>> channelConsumers = Collections.<Consumer<NetChannel<IN,
				OUT>>>singletonList(
				new Consumer<NetChannel<IN, OUT>>() {
					@Override
					public void accept(NetChannel<IN, OUT> channel) {
						channel.consume(inputConsumer);
					}
				});
		return consume(channelConsumers);
	}

	/**
	 * Callback to invoke when a new channel is created.
	 *
	 * @param channelConsumer
	 * 		The callback to invoke for new channels.
	 *
	 * @return {@literal this}
	 */
	public S consume(@Nonnull Consumer<NetChannel<IN, OUT>> channelConsumer) {
		return consume(Collections.singletonList(channelConsumer));
	}

	/**
	 * Callbacks to invoke when a new channel is created.
	 *
	 * @param channelConsumers
	 * 		The callbacks to invoke for new channels.
	 *
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public S consume(@Nonnull Collection<Consumer<NetChannel<IN, OUT>>> channelConsumers) {
		Assert.notNull(channelConsumers, "Connection consumers cannot be null.");
		this.channelConsumers = channelConsumers;
		return (S)this;
	}

}
