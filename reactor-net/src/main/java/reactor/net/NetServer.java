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

package reactor.net;

import reactor.rx.Promise;

import javax.annotation.Nullable;

/**
 * A network-aware server.
 *
 * @author Jon Brisbin
 */
public interface NetServer<IN, OUT> extends Iterable<NetChannel<IN, OUT>> {

	/**
	 * Start and bind this {@literal NetServer} to the configured listen port.
	 *
	 * @return a {@link reactor.rx.Promise} that will be complete when the {@link NetServer} is started
	 */
	Promise<Boolean> start();

	/**
	 * Start and bind this {@literal NetServer} to the configured listen port and notify the given {@link
	 * reactor.fn.Consumer} when the bind operation is complete.
	 *
	 * @param started
	 * 		{@link java.lang.Runnable} to invoke when bind operation is complete
	 *
	 * @return {@link this}
	 */
	NetServer<IN, OUT> start(@Nullable Runnable started);

	/**
	 * Shutdown this {@literal NetServer} and complete the returned {@link reactor.rx.Promise} when shut
	 * down.
	 *
	 * @return a {@link reactor.rx.Promise} that will be complete when the {@link NetServer} is shut down
	 */
	Promise<Boolean> shutdown();

}
