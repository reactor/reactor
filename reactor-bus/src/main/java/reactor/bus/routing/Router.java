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

package reactor.bus.routing;

import reactor.bus.registry.Registration;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;

import java.util.List;

/**
 * An {@code Router} is used to route an {@code Object} to {@link Consumer Consumers}.
 *
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public interface Router<K, V> {

	/**
	 * Routes the {@code event}, triggered by a notification with the given {@code key} to the
	 * {@code consumers}. Depending on the router implementation, zero or more of the consumers
	 * will receive the event. Upon successful completion of the event routing, the
	 * {@code completionConsumer} will be invoked. {@code completionConsumer} may be null. In the
	 * event of an error during routing the {@code errorConsumer} is invoked.
	 * {@code errorConsumer} may be null, in which case the error is swallowed.
	 *
	 * @param key                The notification key
	 * @param data               The {@code Object} to route
	 * @param consumers          The {@code Consumer}s to route the event to.
	 * @param completionConsumer The {@code Consumer} to invoke upon successful completion of event routing
	 * @param errorConsumer      The {@code Consumer} to invoke when an error occurs during event routing
	 */
	<E extends V> void route(K key, E data,
													 List<Registration<K, ? extends BiConsumer<K, ? extends V>>> consumers,
													 Consumer<E> completionConsumer,
													 Consumer<Throwable> errorConsumer);

}