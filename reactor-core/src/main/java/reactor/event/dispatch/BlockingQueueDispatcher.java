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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.pool.LoadingPool;
import reactor.pool.Pool;
import reactor.event.Event;
import reactor.function.Supplier;
import reactor.queue.BlockingQueueFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of {@link Dispatcher} that uses a {@link BlockingQueue} to queue tasks to be executed.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
@SuppressWarnings("rawtypes")
public final class BlockingQueueDispatcher extends BaseLifecycleDispatcher {

	private static final AtomicInteger INSTANCE_COUNT = new AtomicInteger();

	private final ThreadGroup         threadGroup = new ThreadGroup("eventloop");
	private final BlockingQueue<Task> taskQueue   = BlockingQueueFactory.createQueue();
	private final Pool<Task> readyTasks;
	private final Thread     taskExecutor;

	/**
	 * Creates a new {@literal BlockingQueueDispatcher} with the given {@literal name} and {@literal backlog}.
	 *
	 * @param name    The name
	 * @param backlog The backlog size
	 */
	public BlockingQueueDispatcher(String name, int backlog) {
		this.readyTasks = new LoadingPool<Task>(
				new Supplier<Task>() {
					@Override
					public Task get() {
						return new BlockingQueueTask();
					}
				},
				backlog,
				150l
		);
		String threadName = name + "-dispatcher-" + INSTANCE_COUNT.incrementAndGet();

		this.taskExecutor = new Thread(threadGroup, new TaskExecutingRunnable(), threadName);
		this.taskExecutor.setDaemon(true);
		this.taskExecutor.setPriority(Thread.NORM_PRIORITY);
		this.taskExecutor.start();
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		if(taskQueue.isEmpty()) {
			shutdown();
			return true;
		}
		synchronized(taskQueue) {
			try {
				taskQueue.wait();
			} catch(InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			}
		}
		shutdown();
		return true;
	}

	@Override
	public void shutdown() {
		taskExecutor.interrupt();
		super.shutdown();
	}

	@Override
	public void halt() {
		taskExecutor.interrupt();
		super.halt();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <E extends Event<?>> Task<E> createTask() {
		Task t = readyTasks.allocate();
		return (null != t ? t : new BlockingQueueTask());
	}

	private class BlockingQueueTask<E extends Event<?>> extends Task<E> {
		@Override
		public void submit() {
			taskQueue.add(this);
		}
	}

	private class TaskExecutingRunnable implements Runnable {
		@Override
		public void run() {
			Task t = null;
			for (; ; ) {
				try {
					t = taskQueue.poll(200, TimeUnit.MILLISECONDS);
					if (null != t) {
						t.execute();
					}
				} catch (InterruptedException e) {
					break;
				} catch (Exception e) {
					Logger log = LoggerFactory.getLogger(BlockingQueueDispatcher.class);
					if (log.isErrorEnabled()) {
						log.error(e.getMessage(), e);
					}
				} finally {
					if (null != t) {
						t.reset();
						readyTasks.deallocate(t);
					}
					if (taskQueue.isEmpty()) {
						synchronized (taskQueue) {
							taskQueue.notifyAll();
						}
					}
				}
			}
			Thread.currentThread().interrupt();
		}
	}

}
