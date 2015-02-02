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

import org.reactivestreams.Publisher;
import reactor.fn.Function;
import reactor.rx.Promise;
import reactor.rx.Stream;

/**
 * A network-aware client that will publish its connection once available and complete on shutdown.
 *
 * @param <IN> the type of the received data
 * @param <OUT> the type of replied data
 * @param <CONN> the channel implementation
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public interface Client<IN, OUT, CONN extends Channel<IN,OUT>> extends Publisher<CONN> {

	/**
	 * Open a channel to the configured address and return a {@link reactor.rx.Promise} that will be
	 * fulfilled with the connected {@link Channel}.
	 *
	 * @return {@link reactor.rx.Promise} that will be completed when connected
	 */
	Promise<ChannelStream<IN, OUT>> open();

	/**
	 * Open a channel to the configured address and return a {@link reactor.rx..Stream} that will be populated
	 * by the {@link ChannelStream} every time a connection or reconnection is made.
	 *
	 * @param reconnect
	 * 		the reconnection strategy to use when disconnects happen
	 *
	 * @return a Stream of reconnected connections
	 */
	Stream<ChannelStream<IN, OUT>> open(Reconnect reconnect);

	/**
	 * Close this client and the underlying channel.
	 * @return a Promise successful when closed
	 */
	Promise<Boolean> close();

	/**
	 * A global handling pipeline that will be called on each new connection and will listen for signals emitted
	 * by the returned Publisher to write back.
	 *
	 * @return this
	 */
	Client<IN, OUT, CONN> pipeline(Function<CONN, ? extends Publisher<? extends OUT>> serviceFunction);


}
