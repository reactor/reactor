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

public abstract class BaseDispatcher implements Dispatcher {

	@Override
	public <E extends Event<?>> void dispatch(E event,
																						EventRouter eventRouter,
																						Consumer<E> consumer,
																						Consumer<Throwable> errorConsumer) {
		dispatch(null, event, null, errorConsumer, eventRouter, consumer);
	}

	@Override
	public <E extends Event<?>> void dispatch(Object key,
																						E event,
																						Registry<Consumer<? extends Event<?>>> consumerRegistry,
																						Consumer<Throwable> errorConsumer,
																						EventRouter eventRouter,
																						Consumer<E> completionConsumer) {
		if (!alive()) {
			throw new IllegalStateException("This Dispatcher has been shutdown");
		}

		Task<E> task = createTask();

		task.setKey(key);
		task.setEvent(event);
		task.setConsumerRegistry(consumerRegistry);
		task.setErrorConsumer(errorConsumer);
		task.setEventRouter(eventRouter);
		task.setCompletionConsumer(completionConsumer);

		task.submit();
	}

	protected abstract <E extends Event<?>> Task<E> createTask();

	protected abstract class Task<E extends Event<?>> {

		private volatile Object                                 key;
		private volatile Registry<Consumer<? extends Event<?>>> consumerRegistry;
		private volatile E                                      event;
		private volatile Consumer<E>                            completionConsumer;
		private volatile Consumer<Throwable>                    errorConsumer;
		private volatile EventRouter                            eventRouter;

		Task<E> setKey(Object key) {
			this.key = key;
			return this;
		}

		Task<E> setConsumerRegistry(Registry<Consumer<? extends Event<?>>> consumerRegistry) {
			this.consumerRegistry = consumerRegistry;
			return this;
		}

		Task<E> setEvent(E event) {
			this.event = event;
			return this;
		}

		Task<E> setCompletionConsumer(Consumer<E> completionConsumer) {
			this.completionConsumer = completionConsumer;
			return this;
		}

		Task<E> setErrorConsumer(Consumer<Throwable> errorConsumer) {
			this.errorConsumer = errorConsumer;
			return this;
		}

		Task<E> setEventRouter(EventRouter eventRouter) {
			this.eventRouter = eventRouter;
			return this;
		}

		protected void reset() {
			key = null;
			consumerRegistry = null;
			event = null;
			completionConsumer = null;
			errorConsumer = null;
		}

		protected abstract void submit();

		protected void execute() {
			eventRouter.route(key,
												event,
												(null != consumerRegistry ? consumerRegistry.select(key) : null),
												completionConsumer,
												errorConsumer);
		}
	}

}
