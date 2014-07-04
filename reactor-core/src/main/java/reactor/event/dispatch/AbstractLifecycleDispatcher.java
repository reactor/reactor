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

import reactor.alloc.Recyclable;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.routing.Router;
import reactor.function.Consumer;
import reactor.util.Assert;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@code Dispatcher} that has a lifecycle.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class AbstractLifecycleDispatcher implements Dispatcher {

	private static final Router COMPLETION_CONSUMER_EVENT_ROUTER = new Router() {
		@Override
		public <E> void route(Object key,
		                  E event,
		                  List<Registration<? extends Consumer<?>>> consumers,
		                  Consumer<E> completionConsumer,
		                  Consumer<Throwable> errorConsumer) {
			completionConsumer.accept(event);
		}
	};

	private final AtomicBoolean alive   = new AtomicBoolean(true);
	private final ClassLoader   context = new ClassLoader(Thread.currentThread()
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
	public void halt() {
		alive.compareAndSet(true, false);
	}

	/**
	 * Dispatchers can be traced through a {@code contextClassLoader} to let producers adapting their dispatching
	 * strategy
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
	public final <E> void dispatch(E event,
	                               Router router,
	                               Consumer<E> consumer,
	                               Consumer<Throwable> errorConsumer) {
		dispatch(null, event, null, errorConsumer, router, consumer);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <E> void dispatch(Object key,
	                         E event,
	                         Registry<Consumer<?>> consumerRegistry,
	                         Consumer<Throwable> errorConsumer,
	                         Router router,
	                         Consumer<E> completionConsumer) {
		dispatch(key, event, consumerRegistry, errorConsumer, router, completionConsumer, isInContext());
	}

	@SuppressWarnings("unchecked")
	public  <E> void dispatch(Object key,
	                         E event,
	                         Registry<Consumer<?>> consumerRegistry,
	                         Consumer<Throwable> errorConsumer,
	                         Router router,
	                         Consumer<E> completionConsumer, boolean isInContext) {
		Assert.isTrue(alive(), "This Dispatcher has been shut down.");

		try {
			Task task;
			if (isInContext) {
				task = allocateRecursiveTask();
			} else {
				task = allocateTask();
			}

			task.setKey(key)
					.setData(event)
					.setConsumerRegistry(consumerRegistry)
					.setErrorConsumer(errorConsumer)
					.setRouter(router);

			if (completionConsumer != null)
				task.setCompletionConsumer((Consumer<Object>) completionConsumer);

			if (!isInContext) {
				execute(task);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public void execute(final Runnable command) {
		dispatch(null, COMPLETION_CONSUMER_EVENT_ROUTER, new Consumer<Object>() {
			@Override
			public void accept(Object ev) {
				command.run();
			}
		}, null);
	}

	protected abstract Task allocateRecursiveTask();

	protected abstract Task allocateTask();

	protected abstract void execute(Task task);

	protected static void route(Task task) {
		if (null == task.router) {
			return;
		}
		try {
			task.router.route(
					task.key,
					task.data,
					(null != task.consumerRegistry ? task.consumerRegistry.select(task.key) : null),
					task.completionConsumer,
					task.errorConsumer
			);
		} finally {
			task.recycle();
		}
	}

	public abstract class Task implements Runnable, Recyclable {

		protected volatile Object                key;
		protected volatile Registry<Consumer<?>> consumerRegistry;
		protected volatile Object                data;
		protected volatile Consumer<Object>      completionConsumer;
		protected volatile Consumer<Throwable>   errorConsumer;
		protected volatile Router                router;

		public Task setKey(Object key) {
			this.key = key;
			return this;
		}

		public Task setConsumerRegistry(Registry<Consumer<?>> consumerRegistry) {
			this.consumerRegistry = consumerRegistry;
			return this;
		}

		public Task setData(Object data) {
			this.data = data;
			return this;
		}

		public Task setCompletionConsumer(Consumer<Object> completionConsumer) {
			this.completionConsumer = completionConsumer;
			return this;
		}

		public Task setErrorConsumer(Consumer<Throwable> errorConsumer) {
			this.errorConsumer = errorConsumer;
			return this;
		}

		public Task setRouter(Router router) {
			this.router = router;
			return this;
		}

		@Override
		public void recycle() {
			key = null;
			consumerRegistry = null;
			data = null;
			completionConsumer = null;
			errorConsumer = null;
			router = null;
		}

	}

}
