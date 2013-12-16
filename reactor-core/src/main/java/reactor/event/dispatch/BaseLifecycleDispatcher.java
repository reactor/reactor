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

package reactor.event.dispatch;

import reactor.event.Event;
import reactor.event.registry.Registry;
import reactor.event.routing.EventRouter;
import reactor.function.Consumer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@code Dispatcher} that has a lifecycle.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class BaseLifecycleDispatcher implements Dispatcher{

	private final AtomicBoolean alive = new AtomicBoolean(true);
	private final ClassLoader context = new ClassLoader(Thread.currentThread().getContextClassLoader()) {
	};

	protected BaseLifecycleDispatcher() {
		super();
	}

	@Override
	public boolean alive() {
		return alive.get();
	}

	@Override
	public boolean awaitAndShutdown() {
		return awaitAndShutdown(Integer.MAX_VALUE, TimeUnit.SECONDS);
	}

	@Override
	public void shutdown() {
		alive.compareAndSet(true, false);
	}

	@Override
	public void halt() {
		alive.compareAndSet(true, false);
	}

	/**
	 * Dispatchers can be traced through a {@code contextClassLoader} to let producers adapting their
	 * dispatching strategy
	 *
	 * @return boolean true if the programs is already run by this dispatcher
	 */
	protected final boolean isInContext() {
		return context == Thread.currentThread().getContextClassLoader();
	}

	protected final ClassLoader getContext() {
		return context;
	}

	@Override
	public final <E extends Event<?>> void dispatch(E event,
	                                                EventRouter eventRouter,
	                                                Consumer<E> consumer,
	                                                Consumer<Throwable> errorConsumer) {
		dispatch(null, event, null, errorConsumer, eventRouter, consumer);
	}


	protected abstract <E extends Event<?>> Task<E> createTask();

	protected abstract class Task<E extends Event<?>> {

		protected volatile Object                                 key;
		protected volatile Registry<Consumer<? extends Event<?>>> consumerRegistry;
		protected volatile E                                      event;
		protected volatile Consumer<E>                            completionConsumer;
		protected volatile Consumer<Throwable>                    errorConsumer;
		protected volatile EventRouter                            eventRouter;

		final Task<E> setKey(Object key) {
			this.key = key;
			return this;
		}

		final Task<E> setConsumerRegistry(Registry<Consumer<? extends Event<?>>> consumerRegistry) {
			this.consumerRegistry = consumerRegistry;
			return this;
		}

		final Task<E> setEvent(E event) {
			this.event = event;
			return this;
		}

		final Task<E> setCompletionConsumer(Consumer<E> completionConsumer) {
			this.completionConsumer = completionConsumer;
			return this;
		}

		final Task<E> setErrorConsumer(Consumer<Throwable> errorConsumer) {
			this.errorConsumer = errorConsumer;
			return this;
		}

		final Task<E> setEventRouter(EventRouter eventRouter) {
			this.eventRouter = eventRouter;
			return this;
		}

		final protected void reset() {
			key = null;
			consumerRegistry = null;
			event = null;
			completionConsumer = null;
			errorConsumer = null;
		}

		protected void submit() {
		}

		protected abstract void execute();
	}
}
