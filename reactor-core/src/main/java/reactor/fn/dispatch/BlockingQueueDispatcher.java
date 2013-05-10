/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package reactor.fn.dispatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.fn.*;
import reactor.support.QueueFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of {@link Dispatcher} that uses a {@link BlockingQueue} to queue tasks to be executed.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class BlockingQueueDispatcher implements Dispatcher, Linkable<Dispatcher> {

	private final static AtomicInteger THREAD_COUNT = new AtomicInteger();
	private final static Logger        LOG          = LoggerFactory.getLogger(BlockingQueueDispatcher.class);

	private final ThreadGroup      threadGroup       = new ThreadGroup("reactor-dispatcher");
	private final List<Dispatcher> linkedDispatchers = Collections.synchronizedList(new ArrayList<Dispatcher>());
	private final BlockingQueue<Task<?>> readyTasks;
	private final TaskExecutor           taskExecutor;
	private volatile ConsumerInvoker invoker = new ConverterAwareConsumerInvoker();

	public BlockingQueueDispatcher(String name, int backlog) {
		this.readyTasks = QueueFactory.createQueue();
		String threadName = name + "-dispatcher-" + THREAD_COUNT.incrementAndGet();
		this.taskExecutor = new TaskExecutor(threadGroup, threadName);

		for (int i = 0; i < Math.max(backlog, 64); i++) {
			this.readyTasks.add(new BlockingQueueTask<Object>());
		}
		this.start();
	}

	@Override
	public ConsumerInvoker getConsumerInvoker() {
		return invoker;
	}

	@Override
	public BlockingQueueDispatcher setConsumerInvoker(ConsumerInvoker consumerInvoker) {
		this.invoker = consumerInvoker;
		return this;
	}

	@Override
	public BlockingQueueDispatcher link(Dispatcher dispatcher) {
		linkedDispatchers.add(dispatcher);
		return this;
	}

	@Override
	public BlockingQueueDispatcher unlink(Dispatcher dispatcher) {
		linkedDispatchers.remove(dispatcher);
		return this;
	}

	Task<?> steal() {
		return taskExecutor.taskQueue.poll();
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
			taskExecutor.taskQueue.add(this);
		}
	}

	@Override
	public Lifecycle destroy() {
		return this;
	}

	@Override
	public Lifecycle stop() {
		taskExecutor.interrupt();
		return this;
	}

	@Override
	public Lifecycle start() {
		taskExecutor.start();
		return this;
	}

	@Override
	public boolean isAlive() {
		return true;
	}

	private class TaskExecutor extends Thread {
		private final BlockingQueue<Task<?>> taskQueue;

		private TaskExecutor(ThreadGroup group,
												 String name) {
			super(group, name);
			this.taskQueue = QueueFactory.createQueue();
			setDaemon(true);
			setPriority(Thread.MAX_PRIORITY);
		}

		@Override
		@SuppressWarnings({"unchecked"})
		public void run() {
			Task<?> t = null;
			BlockingQueueDispatcher victim = null;
			while (true) {
				try {
					t = taskQueue.poll(200, TimeUnit.MILLISECONDS);
					if (null == t) {
						// No tasks to execute. Try to steal from another Dispatcher
						try {
							for (Dispatcher d : linkedDispatchers) {
								if (d instanceof BlockingQueueDispatcher
										&& null != (t = ((BlockingQueueDispatcher) d).steal())) {
									// Stole a task, execute it.
									victim = (BlockingQueueDispatcher) d;
									break;
								}
							}
						} catch (ConcurrentModificationException ignored) {
							continue;
						}
					}
					if (null == t) {
						// Really no tasks to execute. Try again.
						continue;
					}

					try {
						for (Registration<? extends Consumer<? extends Event<?>>> reg : t.getConsumerRegistry().select(t.getSelector())) {
							if (reg.isCancelled() || reg.isPaused()) {
								continue;
							}
							reg.getSelector().setHeaders(t.getSelector(), t.getEvent());
							invoker.invoke(reg.getObject(), t.getConverter(), Void.TYPE, t.getEvent());
							if (reg.isCancelAfterUse()) {
								reg.cancel();
							}
						}
						if (null != t.getCompletionConsumer()) {
							invoker.invoke(t.getCompletionConsumer(), t.getConverter(), Void.TYPE, t.getEvent());
						}
					} catch (Throwable x) {
						LOG.error(x.getMessage(), x);
						if (null != t.getErrorConsumer()) {
							t.getErrorConsumer().accept(x);
						}
					}
				} catch (InterruptedException e) {
					interrupt();
				} catch (Exception e) {
					LOG.error(e.getMessage(), e);
				} finally {
					if (null != t) {
						t.reset();
						if (null != victim) {
							victim.readyTasks.add(t);
						} else {
							readyTasks.add(t);
						}
					}
				}
			}
		}
	}

}
