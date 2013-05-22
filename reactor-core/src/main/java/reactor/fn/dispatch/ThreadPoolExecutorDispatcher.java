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

import reactor.fn.Cache;
import reactor.fn.ConsumerInvoker;
import reactor.fn.LoadingCache;
import reactor.fn.Supplier;
import reactor.fn.support.ConverterAwareConsumerInvoker;
import reactor.support.NamedDaemonThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import reactor.fn.ConsumerInvoker;
import reactor.fn.support.ConverterAwareConsumerInvoker;
import reactor.support.NamedDaemonThreadFactory;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * A {@code Dispatcher} that uses a {@link ThreadPoolExecutor} with an unbounded queue to execute {@link Task Tasks}.
 *
 * @author Andy Wilkinson
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public final class ThreadPoolExecutorDispatcher implements Dispatcher {

	private static final int DEFAULT_POOL_SIZE = Runtime.getRuntime().availableProcessors();
	private static final int DEFAULT_BACKLOG   = Integer.parseInt(System.getProperty("reactor.dispatcher.backlog", "128"));

	private final ExecutorService executor;
	private final ConsumerInvoker invoker = new ConverterAwareConsumerInvoker();
	private final Cache<ThreadPoolTask> readyTasks;

	/**
	 * Creates a new {@literal ThreadPoolExecutorDispatcher} that will use the default backlog, as configured by the {@code
	 * reactor.dispatcher.backlog} system property (if the property is not set, a backlog of 128 is used), and the default
	 * pool size: the number of available processors.
	 *
	 * @see Runtime#availableProcessors()
	 */
	public ThreadPoolExecutorDispatcher() {
		this(DEFAULT_POOL_SIZE, DEFAULT_BACKLOG);
	}

	/**
	 * Creates a new {@literal ThreadPoolExecutorDispatcher} with the given {@literal poolSize} and {@literal backlog}.
	 *
	 * @param poolSize the pool size
	 * @param backlog  the backlog size
	 */
	public ThreadPoolExecutorDispatcher(int poolSize, int backlog) {
		this.executor = Executors.newFixedThreadPool(
				poolSize,
				new NamedDaemonThreadFactory("thread-pool-executor-dispatcher")
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
	public ThreadPoolExecutorDispatcher destroy() {
		return this;
	}

	@Override
	public ThreadPoolExecutorDispatcher stop() {
		executor.shutdown();
		return this;
	}

	@Override
	public ThreadPoolExecutorDispatcher start() {
		return this;
	}

	@Override
	public boolean isAlive() {
		return executor.isShutdown();
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public <T> Task<T> nextTask() {
		Task t = readyTasks.allocate();
		return (null != t ? t : new ThreadPoolTask());
	}

	private class ThreadPoolTask extends Task<Object> implements Runnable {
		@Override
		public void submit() {
			executor.submit(this);
		}

		@Override
		public void run() {
			try {
				execute(invoker);
			} finally {
				readyTasks.deallocate(this);
			}
		}
	}

}
