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

package reactor.fn.dispatch;

import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.registry.Registry;
import reactor.fn.routing.EventRouter;

/**
 * A {@code Dispatcher} is used to {@link Dispatcher#dispatch(Object, Event, Registry, Consumer,
 * EventRouter, Consumer) dispatch} {@link Event Events} to {@link Consumer Consumers}. The
 * details of how the dispatching is performed, for example on the same thread or using a different
 * thread, are determined by the implementation.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public interface Dispatcher {

	/**
	 * Determine whether this {@code Dispatcher} can be used for
	 * {@link Dispatcher#dispatch(Object, Event, Registry, Consumer, EventRouter, Consumer) dispatching}.
	 *
	 * @return {@literal true} is this {@code Dispatcher} is alive and can be used, {@literal false} otherwise.
	 */
	boolean alive();

	/**
	 * Shutdown this {@code Dispatcher} such that it can no longer be used.
	 */
	void shutdown();

	/**
	 * Shutdown this {@code Dispatcher} and forcibly halt any tasks currently executing, and clear the queues of any
	 * submitted tasks not yet executed.
	 */
	void halt();

	/**
	 * Instruct the {@code Dispatcher} to dispatch the {@code event} that has the given {@code key}.
	 * The {@link Consumer Consumers} that will receive the event are selected from the
	 * {@code consumerRegistry}, and the event is routed to them using the {@code eventRouter}.
	 * In the event of an error during dispatcher, the {@code errorConsumer} will be called. In the
	 * event of successful dispatcher, the {@code completionConsumer} will be called.
	 *
	 * @param key The key associated with the event
	 * @param event The event
	 * @param consumerRegistry The registry from which consumer's are selected
	 * @param errorConsumer The consumer that is driven if dispatch fails
	 * @param eventRouter Used to route the event to the selected consumers
	 * @param completionConsumer The consumer that is driven if dispatch succeeds
	 *
	 * @throws IllegalStateException If the {@code Dispatcher} is not {@link Dispatcher#alive() alive}
	 */
	<E extends Event<?>> void dispatch(Object key, E event, Registry<Consumer<? extends Event<?>>> consumerRegistry, Consumer<Throwable> errorConsumer, EventRouter eventRouter, Consumer<E> completionConsumer);
}
