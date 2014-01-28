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

import reactor.core.alloc.*;
import reactor.event.Event;
import reactor.event.registry.Registry;
import reactor.event.routing.EventRouter;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.queue.BlockingQueueFactory;
import reactor.util.Assert;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class AbstractReferenceCountingDispatcher extends AbstractLifecycleDispatcher {

	private final BlockingQueue<Reference<Task<Event<?>>>> taskQueue;

	private final int                       backlogSize;
	private       Allocator                 tasks;
	private       Allocator<Task<Event<?>>> inContextTasks;

	protected AbstractReferenceCountingDispatcher(int backlogSize) {
		this.backlogSize = backlogSize;
		this.taskQueue = BlockingQueueFactory.createQueue();
		if(backlogSize > 0) {
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
		this.inContextTasks = new ReferenceCountingAllocator<Task<Event<?>>>(
				backlogSize,
				new Supplier<Task<Event<?>>>() {
					@Override
					public Task<Event<?>> get() {
						return createTask();
					}
				}
		);
	}

	protected AbstractReferenceCountingDispatcher(int backlogSize, Allocator tasks) {
		this.backlogSize = backlogSize;
		this.taskQueue = BlockingQueueFactory.createQueue();
		this.tasks = tasks;
		this.inContextTasks = new ReferenceCountingAllocator<Task<Event<?>>>(
				backlogSize,
				new Supplier<Task<Event<?>>>() {
					@Override
					public Task<Event<?>> get() {
						return createTask();
					}
				}
		);
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
		Assert.notNull(tasks, "An Allocator for tasks has not been set. This Dispatcher is unusable until that is done.");
		Assert.isTrue(alive(), "This Dispatcher has already been shut down.");

		boolean isInContext = isInContext();

		Reference<Task<Event<?>>> ref;
		if(isInContext) {
			ref = inContextTasks.allocate();
		} else {
			ref = allocateTaskRef();
		}

		// causes compilation errors with generic types if not cast to raw first
		Consumer _completionConsumer = completionConsumer;
		Task<Event<?>> task = ref.get()
		                         .setKey(key)
		                         .setEvent(event)
		                         .setConsumerRegistry(consumerRegistry)
		                         .setErrorConsumer(errorConsumer)
		                         .setEventRouter(eventRouter)
		                         .setCompletionConsumer((Consumer<Event<?>>)_completionConsumer);

		if(isInContext) {
			taskQueue.add(ref);

			Reference<Task<Event<?>>> nextRef;
			while(null != (nextRef = getTaskQueue().poll())) {
				nextRef.get().execute();
			}
		} else {
			task.submit();
		}
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
		Reference ref = tasks.allocate();
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
			if(isInContext()) {
				execute();
			} else {
				getTaskQueue().add(getRef());
			}
		}

		@Override
		protected void execute() {
			try {
				eventRouter.route(key,
				                  event,
				                  (null != consumerRegistry ? consumerRegistry.select(key) : null),
				                  completionConsumer,
				                  errorConsumer);

				Reference<Task<Event<?>>> nextRef;
				while(null != (nextRef = getTaskQueue().poll())) {
					nextRef.get().execute();
				}
			} finally {
				if(null != ref && ref.getReferenceCount() > 0) {
					ref.release();
				}
			}
		}
	}

	private class EphemeralReference<T extends Recyclable> extends AbstractReference<T> {
		private EphemeralReference(T obj) {
			super(obj);
		}
	}

}
