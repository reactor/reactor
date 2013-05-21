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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.fn.ConsumerInvoker;
import reactor.fn.ConverterAwareConsumerInvoker;
import reactor.support.QueueFactory;

/**
 * Implementation of {@link Dispatcher} that uses a {@link BlockingQueue} to queue tasks to be executed.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class BlockingQueueDispatcher implements Dispatcher {

	private static final int           DEFAULT_BACKLOG = Integer.parseInt(System.getProperty("reactor.dispatcher.backlog", "128"));
	private static final AtomicInteger INSTANCE_COUNT  = new AtomicInteger();

	private final ThreadGroup     threadGroup = new ThreadGroup("reactor-dispatcher");
	private final ConsumerInvoker invoker     = new ConverterAwareConsumerInvoker();

	private final BlockingQueue<Task<?>> readyTasks = QueueFactory.createQueue();
	private final BlockingQueue<Task<?>> taskQueue = QueueFactory.createQueue();
	private final Thread           taskExecutor;

	/**
	 * Creates a new {@literal BlockingQueueDispatcher} named 'blocking-queue' that will use the default backlog,
	 * as configured by the {@code reactor.dispatcher.backlog} system property. If the property is not set,
	 * a backlog of 128 is used.
	 */
	public BlockingQueueDispatcher() {
		this("blocking-queue", DEFAULT_BACKLOG);
	}

	/**
	 * Creates a new {@literal BlockingQueueDispatcher} with the given {@literal name} and {@literal backlog}.
	 *
	 * @param name The name
	 * @param backlog The backlog size
	 */
	public BlockingQueueDispatcher(String name, int backlog) {
		String threadName = name + "-dispatcher-" + INSTANCE_COUNT.incrementAndGet();

		this.taskExecutor = new Thread(threadGroup, new TaskExecutingRunnable(), threadName);
		this.taskExecutor.setDaemon(true);
		this.taskExecutor.setPriority(Thread.MAX_PRIORITY);

		for (int i = 0; i < Math.max(backlog, 64); i++) {
			this.readyTasks.add(new BlockingQueueTask<Object>());
		}
	}

	@Override
	@SuppressWarnings({"unchecked"})
	public <T> Task<T> nextTask() {
		Task<?> t;
		try {
			do {
				t = readyTasks.poll(200, TimeUnit.MILLISECONDS);
			} while (null == t);
			return (Task<T>) t;
		} catch (InterruptedException e) {
			return new BlockingQueueTask<T>();
		}
	}

	private class BlockingQueueTask<T> extends Task<T> {

		@Override
		public void submit() {
			taskQueue.add(this);
		}
	}

	@Override
	public BlockingQueueDispatcher destroy() {
		return this;
	}

	@Override
	public BlockingQueueDispatcher stop() {
		taskExecutor.interrupt();
		return this;
	}

	@Override
	public BlockingQueueDispatcher start() {
		taskExecutor.start();
		return this;
	}

	@Override
	public boolean isAlive() {
		return true;
	}

	private class TaskExecutingRunnable implements Runnable {

		@Override
		public void run() {
			Task<?> t = null;
			while (true) {
				try {
					t = taskQueue.poll(200, TimeUnit.MILLISECONDS);
					if (null != t) {
						t.execute(invoker);
					}
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				} catch (Exception e) {
					Logger log = LoggerFactory.getLogger(BlockingQueueDispatcher.class);
					if (log.isErrorEnabled()) {
						log.error(e.getMessage(), e);
					}
				} finally {
					if (null != t) {
						t.reset();
						readyTasks.add(t);
					}
				}
			}
		}
	}
}
