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

/**
 * A network-aware server that will publish virtual connections (NetChannel) to consume data on.
 * It will complete on shutdown
 *
 * @param <IN> the type of the received data
 * @param <OUT> the type of replied data
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public interface Server<IN, OUT, CONN extends Channel<IN, OUT>> extends Publisher<CONN> {

	/**
	 * Start and bind this {@literal Server} to the configured listen port.
	 *
	 * @return a {@link reactor.rx.Promise} that will be complete when the {@link Server} is started
	 */
	Promise<Boolean> start();

	/**
	 * Shutdown this {@literal Server} and complete the returned {@link reactor.rx.Promise} when shut
	 * down.
	 *
	 * @return a {@link reactor.rx.Promise} that will be complete when the {@link Server} is shut down
	 */
	Promise<Boolean> shutdown();

	/**
	 * A global handling pipeline that will be called on each new connection and will listen for signals emitted
	 * by the returned Publisher to write back.
	 *
	 * @return this
	 */
	Server<IN, OUT, CONN> pipeline(Function<CONN, ? extends Publisher<? extends OUT>> serviceFunction);

}
