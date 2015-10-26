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

import org.reactivestreams.Publisher;
import reactor.fn.tuple.Tuple2;
import reactor.io.net.ChannelStream;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.ReactorChannelHandler;
import reactor.io.net.Reconnect;
import reactor.io.net.config.ClientSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Stream;
import reactor.rx.Streams;

/**
 * A network-aware client that will publish its connection once available to the {@link
 * ReactiveChannelHandler} passed.
 * @param <IN> the type of the received data
 * @param <OUT> the type of replied data
 * @author Stephane Maldini
 */
public final class ReactorTcpClient<IN, OUT>{

	private final TcpClient<IN, OUT> client;

	/**
	 *
	 * @param peer
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	public static <IN, OUT> ReactorTcpClient<IN, OUT> create(TcpClient<IN, OUT> peer) {
		return new ReactorTcpClient<>(peer);
	}

	protected ReactorTcpClient(TcpClient<IN, OUT> client) {
		this.client = client;
	}

	/**
	 *
	 * @param handler
	 * @return
	 */
	public Promise<Void> start(ReactorChannelHandler<IN, OUT> handler) {
		return Promises.from(client.start(
				ChannelStream.wrap(handler, client.getDefaultTimer(), client.getDefaultPrefetchSize())
		));
	}

	/**
	 *
	 * @return
	 */
	public Promise<Void> shutdown() {
		return Promises.from(client.shutdown());
	}

	/**
	 *
	 * @return
	 */
	public InetSocketAddress getConnectAddress() {
		return client.getConnectAddress();
	}


	/**
	 * Open a channel to the configured address and return a {@link Publisher} that will
	 * be populated by the {@link ReactiveChannel} every time a connection or reconnection
	 * is made. <p> The returned {@link Publisher} will typically complete when all
	 * reconnect options have been used, or error if anything wrong happened during the
	 * (re)connection process.
	 * @param reconnect the reconnection strategy to use when disconnects happen
	 * @return a Publisher of reconnected address and accumulated number of attempt pairs
	 */
	public Stream<Tuple2<InetSocketAddress, Integer>> start(
			ReactorChannelHandler<IN, OUT> handler, Reconnect reconnect) {
		return Streams.wrap(
				client.start(
				ChannelStream.wrap(handler, client.getDefaultTimer(), client.getDefaultPrefetchSize())
				, reconnect)
		);
	}


	@SuppressWarnings("unchecked")
	public TcpClient<IN, OUT> delegate() {
		return client;
	}


}
