/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

package reactor.fn.routing;

import java.util.List;

import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.registry.Registration;

/**
 * An {@code EventRouter} is used to route an {@code Event} to {@link Consumer Consumers}.
 *
 * @author Andy Wilkinson
 *
 */
public interface EventRouter {

	/**
	 * Routes the {@code event}, triggered by a notification with the given {@code key} to the
	 * {@code consumers}. Depending on the router implementation, zero or more of the consumers
	 * will receive the event. Upon successful completion of the event routing, the
	 * {@code completionConsumer} will be invoked. {@code completionConsumer} may be null. In the
	 * event of an exception during routing the {@code errorConsumer} is invoked.
	 * {@code errorConsumer} may be null, in which case the exception is swallowed.
	 *
	 * @param key The notification key
	 * @param event The {@code Event} to route
	 * @param consumers The {@code Consumer}s to route the event to.
	 * @param completionConsumer The {@code Consumer} to invoke upon successful completion of event routing
	 * @param errorConsumer The {@code Consumer} to invoke when an error occurs during event routing
	 */
	void route(Object key, Event<?> event, List<Registration<? extends Consumer<? extends Event<?>>>> consumers, Consumer<?> completionConsumer, Consumer<Throwable> errorConsumer);

}