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

import java.net.InetSocketAddress;

import org.reactivestreams.Publisher;
import reactor.Publishers;
import reactor.fn.timer.Timer;
import reactor.fn.tuple.Tuple2;

/**
 * A network-aware client that will publish its connection once available to the {@link
 * ReactiveChannelHandler} passed.
 * @param <IN> the type of the received data
 * @param <OUT> the type of replied data
 * @param <CONN> the channel implementation
 * @author Stephane Maldini
 */
public abstract class ReactiveClient<IN, OUT, CONN extends ReactiveChannel<IN, OUT>>
		extends ReactivePeer<IN, OUT, CONN> {

	public static final ReactiveChannelHandler PING = new ReactiveChannelHandler() {
		@Override
		public Object apply(Object o) {
			return Publishers.empty();
		}
	};

	protected ReactiveClient(Timer defaultEnv, long prefetch) {
		super(defaultEnv, prefetch);
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
	public final Publisher<Tuple2<InetSocketAddress, Integer>> start(
			ReactiveChannelHandler<IN, OUT, CONN> handler, Reconnect reconnect) {
		if (!started.compareAndSet(false, true)) {
			throw new IllegalStateException("Client already started");
		}

		return doStart(handler, reconnect);
	}

	protected abstract Publisher<Tuple2<InetSocketAddress, Integer>> doStart(
			ReactiveChannelHandler<IN, OUT, CONN> handler, Reconnect reconnect);


}
