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

import reactor.cache.Cache;
import reactor.cache.LoadingCache;
import reactor.event.Event;
import reactor.function.Supplier;
import reactor.support.NamedDaemonThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A {@code Dispatcher} that uses a {@link ThreadPoolExecutor} with an unbounded queue to dispatch events.
 *
 * @author Andy Wilkinson
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public final class ThreadPoolExecutorDispatcher extends BaseLifecycleDispatcher {

	private final ExecutorService       executor;
	private final Cache<ThreadPoolTask> readyTasks;

	/**
	 * Creates a new {@literal ThreadPoolExecutorDispatcher} with the given {@literal poolSize} and {@literal backlog}.
	 *
	 * @param poolSize the pool size
	 * @param backlog  the backlog size
	 */
	public ThreadPoolExecutorDispatcher(int poolSize, int backlog) {
		this(poolSize, backlog, "thread-pool-executor-dispatcher");
	}

	public ThreadPoolExecutorDispatcher(int poolSize, int backlog, String threadName) {
		this.executor = Executors.newFixedThreadPool(
				poolSize,
				new NamedDaemonThreadFactory(threadName)
		);
		this.readyTasks = new LoadingCache<ThreadPoolTask>(
				new Supplier<ThreadPoolTask>() {
					@Override
					public ThreadPoolTask get() {
						return new ThreadPoolTask();
					}
				},
				backlog,
				200l
		);
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		shutdown();
		try {
			return executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return false;
	}

	@Override
	public void shutdown() {
		executor.shutdown();
		super.shutdown();
	}

	@Override
	public void halt() {
		executor.shutdownNow();
		super.halt();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <E extends Event<?>> Task<E> createTask() {
		Task<E> t = (Task<E>) readyTasks.allocate();
		return (null != t ? t : (Task<E>) new ThreadPoolTask());
	}

	private class ThreadPoolTask extends Task<Event<Object>> implements Runnable {
		@Override
		public void submit() {
			executor.submit(this);
		}

		@Override
		public void run() {
			try {
				execute();
			} finally {
				readyTasks.deallocate(this);
			}
		}
	}

}
