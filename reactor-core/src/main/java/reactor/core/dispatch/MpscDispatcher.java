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

package reactor.core.dispatch;

import reactor.queue.internal.MpscLinkedQueue;
import reactor.support.NamedDaemonThreadFactory;

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Implementation of a {@link reactor.core.Dispatcher} that uses a {@link reactor.queue.internal.MpscLinkedQueue} to
 * queue tasks to execute.
 *
 * @author Stephane Maldini
 */
public final class MpscDispatcher extends SingleThreadDispatcher {

	private static final int DEFAULT_BUFFER_SIZE = 1024;

	//private final Logger log = LoggerFactory.getLogger(getClass());
	private final ExecutorService executor;
	private final Queue<Task>     workQueue;
	private final int             capacity;

	/**
	 * Creates a new {@code RingBufferDispatcher} with the given {@code name}. It will use a RingBuffer with 1024 slots,
	 * configured with a producer type of {@link reactor.jarjar.com.lmax.disruptor.dsl.ProducerType#MULTI MULTI} and a
	 * {@link reactor.jarjar.com.lmax.disruptor.BlockingWaitStrategy blocking
	 * wait
	 * strategy}.
	 *
	 * @param name The name of the dispatcher.
	 */
	public MpscDispatcher(String name) {
		this(name, DEFAULT_BUFFER_SIZE);
	}

	/**
	 * Creates a new {@literal RingBufferDispatcher} with the given {@code name}. It will use a {@link reactor.jarjar
	 * .com.lmax.disruptor.RingBuffer} with
	 * {@code bufferSize} slots, configured with a producer type of {@link reactor.jarjar.com.lmax.disruptor.dsl
	 * .ProducerType#MULTI MULTI}
	 * and a {@link reactor.jarjar.com.lmax.disruptor.BlockingWaitStrategy blocking wait. A given @param
	 * uncaughtExceptionHandler} will catch anything not
	 * handled e.g. by the owning {@link reactor.event.EventBus} or {@link reactor.rx.Stream}.
	 *
	 * @param name       The name of the dispatcher
	 * @param bufferSize The size to configure the ring buffer with
	 */
	@SuppressWarnings({"unchecked"})
	public MpscDispatcher(String name,
	                      int bufferSize) {
		super(bufferSize);

		this.executor = Executors.newSingleThreadExecutor(new NamedDaemonThreadFactory(name, getContext()));
		this.workQueue = MpscLinkedQueue.create();
		this.capacity = bufferSize;
		this.executor.execute(new Runnable() {
			@Override
			public void run() {
				Task task;
				try {
					for (; ; ) {
						task = workQueue.poll();
						if (null != task) {
							task.run();
						} else {
							LockSupport.parkNanos(1l); //TODO expose
						}
					}
				}catch (EndException e){
					//ignore
				}
			}
		});
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		shutdown();
		try {
			executor.awaitTermination(timeout, timeUnit);
		} catch (InterruptedException e) {
			return false;
		}
		return true;
	}

	@Override
	public void shutdown() {
		workQueue.add(new EndMpscTask());
		executor.shutdown();
		super.shutdown();
	}

	@Override
	public void forceShutdown() {
		workQueue.add(new EndMpscTask());
		executor.shutdownNow();
		super.forceShutdown();
	}

	@Override
	public long remainingSlots() {
		return workQueue.size();
	}


	@Override
	protected Task tryAllocateTask() throws InsufficientCapacityException {
		if (workQueue.size() > capacity) {
			throw InsufficientCapacityException.INSTANCE;
		} else {
			return allocateTask();
		}
	}

	@Override
	protected Task allocateTask() {
		return new SingleThreadTask();
	}

	protected void execute(Task task) {
		workQueue.add(task);
	}

	private class EndMpscTask extends SingleThreadTask {

		@Override
		public void run() {
			throw EndException.INSTANCE;
		}
	}

	private static final class EndException extends IllegalStateException{
		public static final EndException INSTANCE = new EndException();

		private EndException()
		{
			// Singleton
		}

		@Override
		public synchronized Throwable fillInStackTrace()
		{
			return this;
		}
	}

}
