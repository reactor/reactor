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

package reactor.io.net.tcp;

import java.net.InetSocketAddress;

import reactor.io.net.ChannelStream;
import reactor.io.net.Preprocessor;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactivePeer;
import reactor.io.net.ReactorChannelHandler;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.rx.Promise;
import reactor.rx.Promises;

/**
 * A network-aware client that will publish its connection once available to the {@link ReactiveChannelHandler} passed.
 *
 * @param <IN>   the type of the received data
 * @param <OUT>  the type of replied data
 * @author Stephane Maldini
 * @since 2.1
 */
public final class ReactorTcpServer<IN, OUT>  {

	protected final TcpServer<IN, OUT> peer;

	/**
	 *
	 * @param peer
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> ReactorTcpServer<IN, OUT> create(TcpServer<IN, OUT> peer){
		return new ReactorTcpServer<>(peer);
	}

	protected ReactorTcpServer(TcpServer<IN, OUT> peer) {
		this.peer = peer;
	}

	/**
	 * Start this {@literal ReactorPeer}.
	 * @return a {@link Promise<Void>} that will be complete when the {@link
	 * ReactivePeer} is started
	 */
	public Promise<Void> start(ReactorChannelHandler<IN, OUT> handler) {
		return Promises.from(peer.start(
				ChannelStream.wrap(handler, peer.getDefaultTimer(), peer.getDefaultPrefetchSize()))
		);
	}

	/**
	 * Shutdown this {@literal ReactorPeer} and complete the returned {@link Promise<Void>}
	 * when shut down.
	 * @return a {@link Promise<Void>} that will be complete when the {@link
	 * ReactivePeer} is shutdown
	 */
	public Promise<Void> shutdown() {
		return Promises.from(peer.shutdown());
	}

	/**
	 * 
	 * @return
	 */
	public InetSocketAddress getListenAddress() {
		return peer.getListenAddress();
	}

	/**
	 *
	 * @return
	 */
	public TcpServer<IN, OUT> delegate() {
		return peer;
	}
}
