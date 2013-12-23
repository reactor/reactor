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

package reactor.spring.core.task;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.routing.EventRouter;
import reactor.event.selector.Selector;
import reactor.function.Consumer;

/**
 * An adapter that allows a Reactor {@link Dispatcher} to be used as a Spring {@link
 * TaskExecutor}.
 *
 * @author Jon Brisbin
 */
public class DispatcherTaskExecutor implements TaskExecutor {

	private final Registry<Consumer<? extends Event<?>>> consumerRegistry = new EmptyConsumerRegistry();
	private final EventRouter                            eventRouter      = new RunnableEventRouter();
	private final Dispatcher dispatcher;

	/**
	 * Creates a new DispatcherTaskExceutor that will use the given {@code dispatcher} to execute
	 * tasks.
	 *
	 * @param dispatcher
	 * 		The dispatcher to use
	 */
	@Autowired
	public DispatcherTaskExecutor(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	@Override
	public void execute(Runnable task) {
		dispatcher.dispatch(null, Event.wrap(task), consumerRegistry, null, eventRouter, null);
	}

	private static final class RunnableEventRouter implements EventRouter {
		@Override
		public void route(Object key,
		                  Event<?> event,
		                  List<Registration<? extends Consumer<? extends Event<?>>>> consumers,
		                  Consumer<?> completionConsumer,
		                  Consumer<Throwable> errorConsumer) {
			((Runnable)event.getData()).run();
		}
	}

	private static final class EmptyConsumerRegistry implements Registry<Consumer<? extends Event<?>>> {
		@Override
		public Iterator<Registration<? extends Consumer<? extends Event<?>>>> iterator() {
			throw new UnsupportedOperationException();
		}

		@Override
		public <V extends Consumer<? extends Event<?>>> Registration<V> register(Selector sel, V obj) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean unregister(Object key) {
			throw new UnsupportedOperationException();
		}

		@Override
		public List<Registration<? extends Consumer<? extends Event<?>>>> select(Object key) {
			return Collections.emptyList();
		}

		@Override
		public void clear() {
		}
	}

}
