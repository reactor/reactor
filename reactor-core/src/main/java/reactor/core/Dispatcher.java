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

package reactor.core;

import reactor.core.dispatch.InsufficientCapacityException;
import reactor.fn.Consumer;
import reactor.fn.Resource;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * A {@code Dispatcher} is used to {@link Dispatcher#dispatch(Object, Consumer, Consumer)}
 * dispatch} data to {@link Consumer}s. The details of how the dispatching is performed, for example on the
 * same thread or using a different thread, are determined by the implementation.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public interface Dispatcher extends Executor, Resource {

	/**
	 * Instruct the {@code Dispatcher} to dispatch the {@code data}. The event {@link Consumer}
	 *  will receive the event. In the event of an error during dispatching, the {@code errorConsumer} will be called.
	 *
	 * @param data               The event
	 * @param eventConsumer      The consumer that is driven if dispatch succeeds
	 * @param errorConsumer      The consumer that is invoked if dispatch fails. May be {@code null}
	 * @param <E>                type of the event
	 * @throws IllegalStateException If the {@code Dispatcher} is not {@link Dispatcher#alive() alive}
	 */
	<E> void dispatch(E data,
	                  Consumer<E> eventConsumer,
	                  Consumer<Throwable> errorConsumer);

	/**
	 * Instruct the {@code Dispatcher} to dispatch the {@code data}.
	 * If the dispatcher doesn't have enough capacity and might block on the next produced event,
	 * The event {@link Consumer}
	 *  will receive the event. In the event of an error during dispatching, the {@code errorConsumer} will be called.
	 *
	 * @param data               The event
	 * @param eventConsumer      The consumer that is driven if dispatch succeeds
	 * @param errorConsumer      The consumer that is invoked if dispatch fails. May be {@code null}
	 * @param <E>                type of the event
	 * @throws IllegalStateException If the {@code Dispatcher} is not {@link Dispatcher#alive() alive}
	 */
	<E> void tryDispatch(E data,
	                  Consumer<E> eventConsumer,
	                  Consumer<Throwable> errorConsumer) throws InsufficientCapacityException;

	/**
	 * Block until all submitted tasks have completed, then do a normal {@link #shutdown()}.
	 */
	boolean awaitAndShutdown();

	/**
	 * Block until all submitted tasks have completed, then do a normal {@link #shutdown()}.
	 */
	boolean awaitAndShutdown(long timeout, TimeUnit timeUnit);


	/**
	 * Request the remaining capacity for the underlying shared state structure.
	 * E.g. {@link reactor.core.dispatch.RingBufferDispatcher} will return
	 * {@link reactor.jarjar.com.lmax.disruptor.RingBuffer#remainingCapacity()}.
	 * <p>
	 *
	 * @return the remaining capacity if supported otherwise it returns a negative value.
	 * @since 2.0
	 */
	long remainingSlots();

	/**
	 * Request the capacity for the underlying shared state structure.
	 * E.g. {@link reactor.core.dispatch.RingBufferDispatcher} will return
	 * {@link reactor.jarjar.com.lmax.disruptor.RingBuffer#getBufferSize()}.
	 * <p>
	 *
	 * @return the remaining capacity if supported otherwise it returns a negative value.
	 * @since 2.0
	 */
	int backlogSize();


	/**
	 * Inspect if the dispatcher supports ordered dispatching:
	 * Single threaded dispatchers naturally preserve event ordering on dispatch.
	 * Multi threaded dispatchers can't prevent a single consumer to receives concurrent notifications.
	 *
	 * @return true if ordering of dispatch is preserved.
	 * * @since 2.0
	 */
	boolean supportsOrdering();


	/**
	 * A dispatcher context can be bound to the thread(s) it runs on. This method allows any caller to detect if he is
	 * actually within this dispatcher scope.
	 *
	 * @return true if within Dispatcher scope (e.g. a thread).
	 * * @since 2.0
	 */
	boolean inContext();


}
