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

import reactor.core.alloc.AbstractReference;
import reactor.core.alloc.Allocator;
import reactor.core.alloc.Reference;
import reactor.core.alloc.ReferenceCountingAllocator;
import reactor.event.Event;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.routing.EventRouter;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.queue.BlockingQueueFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Base implementation of {@link reactor.event.dispatch.Dispatcher} that provides functionality
 */
public abstract class AbstractReferenceCountingDispatcher extends AbstractLifecycleDispatcher {

	private final ThreadLocal<Reference<Task<Event<?>>>> inContextTask = new
			InheritableThreadLocal<Reference<Task<Event<?>>>>();
	private final BlockingQueue<Reference<Task<Event<?>>>> taskQueue;

	private final int                       backlogSize;
	private       Allocator                 tasks;

	protected AbstractReferenceCountingDispatcher(int backlogSize) {
		this(backlogSize, null);
	}

	protected AbstractReferenceCountingDispatcher(int backlogSize, Allocator tasks) {
		this.backlogSize = backlogSize;
		this.taskQueue = BlockingQueueFactory.createQueue();
		if(null != tasks) {
			this.tasks = tasks;
		} else {
			this.tasks = new ReferenceCountingAllocator<Task>(
					backlogSize,
					new Supplier<Task>() {
						@Override
						public Task get() {
							return createTask();
						}
					}
			);
		}
	}

	public int getBacklogSize() {
		return backlogSize;
	}

	public BlockingQueue<Reference<Task<Event<?>>>> getTaskQueue() {
		return taskQueue;
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		if(!taskQueue.isEmpty()) {
			synchronized(taskQueue) {
				try {
					taskQueue.wait(timeUnit.convert(timeout, TimeUnit.MILLISECONDS));
				} catch(InterruptedException e) {
					Thread.currentThread().interrupt();
					return false;
				}
			}
		}

		shutdown();
		return true;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <E extends Event<?>> void dispatch(Object key,
	                                          E event,
	                                          Registry<Consumer<? extends Event<?>>> consumerRegistry,
	                                          Consumer<Throwable> errorConsumer,
	                                          EventRouter eventRouter,
	                                          Consumer<E> completionConsumer) {
		Reference<Task<Event<?>>> ref = allocateTaskRef();

		// causes compilation errors with generic types if not cast to raw first
		Consumer _completionConsumer = completionConsumer;
		ref.get()
		   .setKey(key)
		   .setEvent(event)
		   .setConsumerRegistry(consumerRegistry)
		   .setErrorConsumer(errorConsumer)
		   .setEventRouter(eventRouter)
		   .setCompletionConsumer((Consumer<Event<?>>)_completionConsumer)
		   .submit();
	}

	protected void setAllocator(Allocator tasks) {
		this.tasks = tasks;
	}

	protected Task createTask() {
		return new SingleThreadTask();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <E extends Event<?>> Reference<Task<E>> allocateTaskRef() {
		Reference ref;
		if(isInContext()) {
			ref = inContextTask.get();
			if(null == ref) {
				Task<Event<?>> task = createTask();
				ref = new AbstractReference(task) {};
				inContextTask.set(ref);
			}
			ref.retain();
		} else {
			ref = tasks.allocate();
		}
		((SingleThreadTask)ref.get()).setRef(ref);
		return (Reference<Task<E>>)ref;
	}

	protected class SingleThreadTask<E extends Event<?>> extends Task<E> {
		private volatile Reference<Task<Event<?>>> ref;

		public Reference<Task<Event<?>>> getRef() {
			return ref;
		}

		public void setRef(Reference<Task<Event<?>>> ref) {
			this.ref = ref;
		}

		@Override
		protected void submit() {
			taskQueue.add(getRef());
		}

		@Override
		protected void execute() {
			Reference<Task<Event<?>>> ref = getRef();
			Task<Event<?>> task = ref.get();
			do {
				try {
					route(task.eventRouter,
					      task.key,
					      task.event,
					      (null != task.consumerRegistry ? task.consumerRegistry.select(task.key) : null),
					      task.completionConsumer,
					      task.errorConsumer);
				} finally {
					if(ref.getReferenceCount() == 1) {
						ref.release();
					}
				}

				ref = taskQueue.poll();
			} while(null != ref);
		}
	}

	@SuppressWarnings("unchecked")
	protected static void route(EventRouter eventRouter,
	                          Object key,
	                          Event event,
	                          List<Registration<? extends Consumer<? extends Event<?>>>> registrations,
	                          Consumer completionConsumer,
	                          Consumer errorConsumer) {
		eventRouter.route(key,
		                  event,
		                  registrations,
		                  completionConsumer,
		                  errorConsumer);
	}

}
