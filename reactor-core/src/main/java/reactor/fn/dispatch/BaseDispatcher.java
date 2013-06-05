/*
 * Copyright (c) 2011-2013 the original author or authors.
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

abstract class BaseDispatcher implements Dispatcher {

	@Override
	public <E extends Event<?>> void dispatch(Object key, E event, Registry<Consumer<? extends Event<?>>> consumerRegistry, Consumer<Throwable> errorConsumer, EventRouter eventRouter, Consumer<E> completionConsumer) {
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

		public Task<E> setKey(Object key) {
			this.key = key;
			return this;
		}

		public Task<E> setConsumerRegistry(Registry<Consumer<? extends Event<?>>> consumerRegistry) {
			this.consumerRegistry = consumerRegistry;
			return this;
		}

		public Task<E> setEvent(E event) {
			this.event = event;
			return this;
		}

		public Task<E> setCompletionConsumer(Consumer<E> completionConsumer) {
			this.completionConsumer = completionConsumer;
			return this;
		}

		public Task<E> setErrorConsumer(Consumer<Throwable> errorConsumer) {
			this.errorConsumer = errorConsumer;
			return this;
		}

		public Task<E> setEventRouter(EventRouter eventRouter) {
			this.eventRouter = eventRouter;
			return this;
		}

		public void reset() {
			key = null;
			consumerRegistry = null;
			event = null;
			completionConsumer = null;
			errorConsumer = null;
		}

		public abstract void submit();

		protected void execute() {
			eventRouter.route(key, event, consumerRegistry.select(key), completionConsumer, errorConsumer);
		}
	}
}
