/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.processor;

import org.reactivestreams.Processor;
import reactor.core.error.Exceptions;
import reactor.core.error.InsufficientCapacityException;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.core.support.SingleUseExecutor;
import reactor.core.support.internal.MpscLinkedQueue;

import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * A base processor used by executor backed processors to take care of their ExecutorService
 *
 * @author Stephane Maldini
 */
public final class SimpleWorkProcessor<IN, OUT> extends ExecutorPoweredProcessor<IN, OUT> {

	protected final ExecutorService executor;

	protected SimpleWorkProcessor(String name, ExecutorService executor, boolean autoCancel) {
		super(autoCancel);

		this.executor = executor == null
				? SingleUseExecutor.create(name)
				: executor;
	}


	@Override
	public void onComplete() {
		if (executor.getClass() == SingleUseExecutor.class) {
			executor.shutdown();
		}
	}

	@Override
	public boolean awaitAndShutdown() {
		return awaitAndShutdown(-1, TimeUnit.SECONDS);
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		try {
			shutdown();
			return executor.awaitTermination(timeout, timeUnit);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			return false;
		}
	}

	@Override
	public void forceShutdown() {
		if (executor.isShutdown()) return;
		executor.shutdownNow();
	}

	@Override
	public boolean alive() {
		return !executor.isTerminated();
	}

	@Override
	public void shutdown() {
		try {
			onComplete();
			executor.shutdown();
		} catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			onError(t);
		}
	}

	private static final int DEFAULT_BUFFER_SIZE = 1024;

	//private final Logger log = LoggerFactory.getLogger(getClass());
	private final ExecutorService executor;
	private final Queue<Task>     workQueue;
	private final int             capacity;

	/**
	 * Creates a new {@code MpscDispatcher} with the given {@code name}. It will use a MpscLinkedQueue and a virtual
	 * capacity of 1024 slots.
	 *
	 * @param name The name of the dispatcher.
	 */
	public MpscDispatcher(String name) {
		this(name, DEFAULT_BUFFER_SIZE);
	}

	/**
	 * Creates a new {@code MpscDispatcher} with the given {@code name}. It will use a MpscLinkedQueue and a virtual
	 * capacity of {code bufferSize}
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
				} catch (EndException e) {
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
			throw InsufficientCapacityException.get();
		} else {
			return allocateTask();
		}
	}

	public  static <T> Processor<T, T> create(String name, int bufferSize) {
	}


	@Override
	public boolean isWork() {
		return true;
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

}
