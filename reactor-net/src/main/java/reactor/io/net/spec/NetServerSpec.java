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

package reactor.io.net.spec;

import reactor.bus.spec.DispatcherComponentSpec;
import reactor.core.support.Assert;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.NetChannel;
import reactor.io.net.NetServer;
import reactor.io.net.config.ServerSocketOptions;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class NetServerSpec<IN, OUT,
		CONN extends NetChannel<IN,OUT>,
		S extends NetServerSpec<IN, OUT, CONN, S, N>,
		N extends NetServer<IN,OUT,CONN>>
		extends DispatcherComponentSpec<S, N> {

	protected ServerSocketOptions options = new ServerSocketOptions();
	protected InetSocketAddress      listenAddress;
	protected Codec<Buffer, IN, OUT> codec;

	/**
	 * Set the common {@link ServerSocketOptions} for channels made in this server.
	 *
	 * @param options The options to set when new channels are made.
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
	 * @param port The port to listen on.
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public S listen(int port) {
		return listen(new InetSocketAddress(port));
	}

	/**
	 * The host and port on which this server should listen.
	 *
	 * @param host The host to bind to.
	 * @param port The port to listen on.
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
	 * @param listenAddress the listen address
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public S listen(InetSocketAddress listenAddress) {
		this.listenAddress = listenAddress;
		return (S) this;
	}

	/**
	 * The {@link Codec} to use to encode and decode data.
	 *
	 * @param codec The codec to use.
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public S codec(@Nonnull Codec<Buffer, IN, OUT> codec) {
		Assert.notNull(codec, "Codec cannot be null.");
		this.codec = codec;
		return (S) this;
	}
}
