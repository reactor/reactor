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
public interface NetServer<IN, OUT, CONN extends NetChannel<IN, OUT>> extends Publisher<CONN> {

	/**
	 * Start and bind this {@literal NetServer} to the configured listen port.
	 *
	 * @return a {@link reactor.rx.Promise} that will be complete when the {@link NetServer} is started
	 */
	Promise<Void> start();

	/**
	 * Shutdown this {@literal NetServer} and complete the returned {@link reactor.rx.Promise} when shut
	 * down.
	 *
	 * @return a {@link reactor.rx.Promise} that will be complete when the {@link NetServer} is shut down
	 */
	Promise<Void> shutdown();

}
