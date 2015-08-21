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

package reactor.io.net;

import reactor.Environment;
import reactor.ReactorProcessor;
import reactor.fn.tuple.Tuple2;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.net.InetSocketAddress;

/**
 * A network-aware client that will publish its connection once available to the {@link ReactorChannelHandler} passed.
 *
 * @param <IN>   the type of the received data
 * @param <OUT>  the type of replied data
 * @param <CONN> the channel implementation
 * @author Stephane Maldini
 */
public abstract class ReactorClient<IN, OUT, CONN extends ChannelStream<IN, OUT>> extends ReactorPeer<IN, OUT, CONN> {

	public static final ReactorChannelHandler PING = new ReactorChannelHandler() {
		@Override
		public Object apply(Object o) {
			return Streams.empty();
		}
	};


	protected ReactorClient(Environment defaultEnv, ReactorProcessor defaultDispatcher, Codec<Buffer, IN, OUT> codec,
	                        long
	  prefetch) {
		super(defaultEnv, defaultDispatcher, codec, prefetch);
	}

	/**
	 * Open a channel to the configured address and return a {@link reactor.rx.Stream} that will be populated
	 * by the {@link ChannelStream} every time a connection or reconnection is made.
	 * <p>
	 * The returned {@link Stream} will typically complete when all reconnect options have been used, or error if
	 * anything
	 * wrong happened during the (re)connection process.
	 *
	 * @param reconnect the reconnection strategy to use when disconnects happen
	 * @return a Stream of reconnected address and accumulated number of attempt pairs
	 */
	public final Stream<Tuple2<InetSocketAddress, Integer>> start(ReactorChannelHandler<IN, OUT, CONN> handler,
	                                                              Reconnect reconnect) {
		if (!started.compareAndSet(false, true)) {
			throw new IllegalStateException("Client already started");
		}

		return doStart(handler, reconnect);
	}

	protected abstract Stream<Tuple2<InetSocketAddress, Integer>> doStart(ReactorChannelHandler<IN, OUT, CONN> handler,
	                                                                      Reconnect reconnect);
}
