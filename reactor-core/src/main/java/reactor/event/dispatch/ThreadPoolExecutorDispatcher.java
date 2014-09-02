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

import reactor.support.NamedDaemonThreadFactory;

import java.util.concurrent.*;

/**
 * A {@code Dispatcher} that uses a {@link ThreadPoolExecutor} with an unbounded queue to dispatch events.
 *
 * @author Andy Wilkinson
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ThreadPoolExecutorDispatcher extends MultiThreadDispatcher {

	private final ExecutorService         executor;
	private final BlockingQueue<Runnable> workQueue;

	/**
	 * Creates a new {@literal ThreadPoolExecutorDispatcher} with the given {@literal poolSize} and {@literal backlog}.
	 * By default, a {@link java.util.concurrent.RejectedExecutionHandler} is created which runs the submitted {@code
	 * Runnable} in the calling thread. To change this behavior, specify your own.
	 *
	 * @param poolSize
	 * 		the pool size
	 * @param backlog
	 * 		the backlog size
	 */
	public ThreadPoolExecutorDispatcher(int poolSize, int backlog) {
		this(poolSize, backlog, "threadPoolExecutorDispatcher");
	}

	/**
	 * Create a new {@literal ThreadPoolExecutorDispatcher} with the given size, backlog, name, and {@link
	 * java.util.concurrent.RejectedExecutionHandler}.
	 *
	 * @param poolSize
	 * 		the pool size
	 * @param backlog
	 * 		the backlog size
	 * @param threadName
	 * 		the name prefix to use when creating threads
	 */
	public ThreadPoolExecutorDispatcher(int poolSize,
	                                    int backlog,
	                                    String threadName) {
		this(poolSize,
		     backlog,
		     threadName,
		     new LinkedBlockingQueue<Runnable>(backlog),
		     new RejectedExecutionHandler() {
			     @Override
			     public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
				     r.run();
			     }
		     });
	}

	/**
	 * Create a new {@literal ThreadPoolExecutorDispatcher} with the given size, backlog, name, and {@link
	 * java.util.concurrent.RejectedExecutionHandler}.
	 *
	 * @param poolSize
	 * 		the pool size
	 * @param backlog
	 * 		the backlog size
	 * @param threadName
	 * 		the name prefix to use when creating threads
	 * @param rejectedExecutionHandler
	 * 		the {@code RejectedExecutionHandler} to use when jobs can't be submitted to the thread pool
	 */
	public ThreadPoolExecutorDispatcher(int poolSize,
	                                    int backlog,
	                                    String threadName,
	                                    BlockingQueue<Runnable> workQueue,
	                                    RejectedExecutionHandler rejectedExecutionHandler) {
		super(poolSize, backlog);
		this.workQueue = workQueue;
		this.executor = new ThreadPoolExecutor(
				poolSize,
				poolSize,
				0L,
				TimeUnit.MILLISECONDS,
				workQueue,
				new NamedDaemonThreadFactory(threadName, getContext()),
				rejectedExecutionHandler
		);
	}

	/**
	 * Create a new {@literal ThreadPoolTaskExecutor} with the given backlog and {@link
	 * java.util.concurrent.ExecutorService}.
	 *
	 * @param backlog
	 * 		the task backlog
	 * @param poolSize
	 * 		the number of threads
	 * @param executor
	 * 		the executor to use to execute tasks
	 */
	public ThreadPoolExecutorDispatcher(int backlog, int poolSize, ExecutorService executor) {
		super(poolSize, backlog);
		this.executor = executor;
		this.workQueue = null;
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		shutdown();
		try {
			if(!executor.awaitTermination(timeout, timeUnit)) {
				return false;
			}
		} catch(InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
		return true;
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

	@Override
	public long remainingSlots() {
		return workQueue != null ? workQueue.remainingCapacity() : Long.MAX_VALUE;
	}

	@Override
	protected void execute(Task task) {
		executor.execute(task);
	}

	@Override
	public void execute(Runnable command) {
		executor.execute(command);
	}

}
