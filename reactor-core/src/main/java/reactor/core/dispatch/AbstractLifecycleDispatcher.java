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

package reactor.core.dispatch;

import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.alloc.Recyclable;
import reactor.core.support.Assert;
import reactor.fn.Consumer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@code Dispatcher} that has a lifecycle.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class AbstractLifecycleDispatcher implements Dispatcher {


	private final AtomicBoolean alive   = new AtomicBoolean(true);
	public final  ClassLoader   context = new ClassLoader(Thread.currentThread()
			.getContextClassLoader()) {
	};

	protected AbstractLifecycleDispatcher() {
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
	public void forceShutdown() {
		alive.compareAndSet(true, false);
	}

	/**
	 * Dispatchers can be traced through a {@code contextClassLoader} to let producers adapting their dispatching
	 * strategy
	 *
	 * @return boolean true if the programs is already run by this dispatcher
	 */
	@Override
	public boolean inContext() {
		return context == Thread.currentThread().getContextClassLoader();
	}

	protected final ClassLoader getContext() {
		return context;
	}

	@Override
	public final <E> void tryDispatch(E event, Consumer<E> eventConsumer, Consumer<Throwable> errorConsumer)
			throws InsufficientCapacityException {
		Assert.isTrue(alive(), "This Dispatcher has been shut down.");
		boolean isInContext = inContext();
		Task task;
		if (isInContext) {
			task = allocateRecursiveTask();
		} else {
			task = tryAllocateTask();
		}

		task.setData(event)
				.setErrorConsumer(errorConsumer)
				.setEventConsumer(eventConsumer);

		if (!isInContext) {
			execute(task);
		}
	}

	@Override
	public final <E> void dispatch(E event,
	                               Consumer<E> eventConsumer,
	                               Consumer<Throwable> errorConsumer) {

		Assert.isTrue(alive(), "This Dispatcher has been shut down.");
		Assert.isTrue(eventConsumer != null, "The signal consumer has not been passed.");
		boolean isInContext = inContext();
		Task task;
		if (isInContext) {
			task = allocateRecursiveTask();
		} else {
			task = allocateTask();
		}

		task.setData(event)
				.setErrorConsumer(errorConsumer)
				.setEventConsumer(eventConsumer);

		if (!isInContext) {
			execute(task);
		}
	}

	@Override
	public void execute(final Runnable command) {
		dispatch(null, new Consumer<Object>() {
			@Override
			public void accept(Object ev) {
				command.run();
			}
		}, null);
	}

	protected Task tryAllocateTask() throws InsufficientCapacityException{
		return allocateTask();
	}

	protected abstract Task allocateTask();

	protected abstract Task allocateRecursiveTask();

	protected abstract void execute(Task task);

	@SuppressWarnings("unchecked")
	protected static void route(Task task) {
		try {
			task.eventConsumer.accept(task.data);

		} catch (Exception e) {
			if (task.errorConsumer != null) {

				task.errorConsumer.accept(e);

			} else if (Environment.alive()) {

				Environment.get().routeError(e);

			}
		} finally {
			task.recycle();
		}
	}

	public abstract class Task implements Runnable, Recyclable {

		protected volatile Object              data;
		protected volatile Consumer            eventConsumer;
		protected volatile Consumer<Throwable> errorConsumer;

		public Task setData(Object data) {
			this.data = data;
			return this;
		}

		public Task setEventConsumer(Consumer<?> eventConsumer) {
			this.eventConsumer = eventConsumer;
			return this;
		}

		public Task setErrorConsumer(Consumer<Throwable> errorConsumer) {
			this.errorConsumer = errorConsumer;
			return this;
		}

		@Override
		public void recycle() {
			data = null;
			errorConsumer = null;
			eventConsumer = null;
		}

	}

}
