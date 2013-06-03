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

/**
 * Abstract class that a {@link Dispatcher} will implement that provides a caller with a holder for the components of an
 * event dispatch. The {@link #submit()} method is called to actually place the task on the internal queue to be
 * executed.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class Task<T> {

	private volatile Object                                 key;
	private volatile Registry<Consumer<? extends Event<?>>> consumerRegistry;
	private volatile Event<T>                               event;
	private volatile Consumer<Event<T>>                     completionConsumer;
	private volatile Consumer<Throwable>                    errorConsumer;
	private volatile EventRouter                            eventRouter;

	public Task<T> setKey(Object key) {
		this.key = key;
		return this;
	}

	public Task<T> setConsumerRegistry(Registry<Consumer<? extends Event<?>>> consumerRegistry) {
		this.consumerRegistry = consumerRegistry;
		return this;
	}

	public Task<T> setEvent(Event<T> event) {
		this.event = event;
		return this;
	}

	public Task<T> setCompletionConsumer(Consumer<Event<T>> completionConsumer) {
		this.completionConsumer = completionConsumer;
		return this;
	}

	public Task<T> setErrorConsumer(Consumer<Throwable> errorConsumer) {
		this.errorConsumer = errorConsumer;
		return this;
	}

	public Task<T> setEventRouter(EventRouter eventRouter) {
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

	/**
	 * Submit this task for execution. Implementations might block on this method if there is no room to queue this task
	 * for execution.
	 */
	public abstract void submit();

	protected void execute() {
		eventRouter.route(key, event, consumerRegistry.select(key), completionConsumer, errorConsumer);
	}

}
