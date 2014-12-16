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

import reactor.fn.Consumer;
import reactor.rx.Promise;
import reactor.rx.Stream;

/**
 * A network-aware client.
 *
 * @author Jon Brisbin
 */
public interface NetClient<IN, OUT> extends Iterable<NetChannel<IN, OUT>> {

	/**
	 * Open a channel to the configured address and return a {@link reactor.rx.Promise} that will be
	 * fulfilled with the connected {@link NetChannel}.
	 *
	 * @return {@link reactor.rx.Promise} that will be completed when connected
	 */
	Promise<NetChannel<IN, OUT>> open();

	/**
	 * Open a channel to the configured address and return a {@link reactor.core.composable.Stream} that will be populated
	 * by the {@link NetChannel NetChannels} every time a connection or reconnection is made.
	 *
	 * @param reconnect
	 * 		the reconnection strategy to use when disconnects happen
	 *
	 * @return
	 */
	Stream<NetChannel<IN, OUT>> open(Reconnect reconnect);

	/**
	 * Close this client and the underlying channel.
	 */
	Promise<Boolean> close();

	/**
	 * Close this client and the underlying channel and invoke the given {@link reactor.fn.Consumer} when the
	 * operation has completed.
	 *
	 * @param onClose
	 * 		consumer to invoke when client is closed
	 */
	void close(Consumer<Boolean> onClose);

}
