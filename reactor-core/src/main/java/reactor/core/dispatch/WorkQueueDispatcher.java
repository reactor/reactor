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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.dispatch.wait.WaitingMood;
import reactor.function.Consumer;
import reactor.jarjar.com.lmax.disruptor.*;
import reactor.jarjar.com.lmax.disruptor.dsl.Disruptor;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;
import reactor.support.NamedDaemonThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of a {@link reactor.core.Dispatcher} that uses a multi-threaded, multi-producer {@link RingBuffer} to queue tasks
 * to execute.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @since 1.1
 */
public class WorkQueueDispatcher extends MultiThreadDispatcher implements WaitingMood {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ExecutorService           executor;
	private final Disruptor<WorkQueueTask>  disruptor;
	private final WaitingMood               waitingMood;
	private final RingBuffer<WorkQueueTask> ringBuffer;

	@SuppressWarnings("unchecked")
	public WorkQueueDispatcher(String name,
	                           int poolSize,
	                           int backlog,
	                           final Consumer<Throwable> uncaughtExceptionHandler) {
		this(name, poolSize, backlog, uncaughtExceptionHandler, ProducerType.MULTI, new BlockingWaitStrategy());
	}

	@SuppressWarnings("unchecked")
	public WorkQueueDispatcher(String name,
	                           int poolSize,
	                           int backlog,
	                           final Consumer<Throwable> uncaughtExceptionHandler,
	                           ProducerType producerType,
	                           WaitStrategy waitStrategy) {
		super(poolSize, backlog);

		if (WaitingMood.class.isAssignableFrom(waitStrategy.getClass())) {
			this.waitingMood = (WaitingMood) waitStrategy;
		} else {
			this.waitingMood = null;
		}

		this.executor = Executors.newFixedThreadPool(
				poolSize,
				new NamedDaemonThreadFactory(name, getContext())
		);
		this.disruptor = new Disruptor<WorkQueueTask>(
				new EventFactory<WorkQueueTask>() {
					@Override
					public WorkQueueTask newInstance() {
						return new WorkQueueTask();
					}
				},
				backlog,
				executor,
				producerType,
				waitStrategy
		);

		this.disruptor.handleExceptionsWith(new ExceptionHandler() {
			@Override
			public void handleEventException(Throwable ex, long sequence, Object event) {
				handleOnStartException(ex);
			}

			@Override
			public void handleOnStartException(Throwable ex) {
				if (null != uncaughtExceptionHandler) {
					uncaughtExceptionHandler.accept(ex);
				} else {
					log.error(ex.getMessage(), ex);
				}
			}

			@Override
			public void handleOnShutdownException(Throwable ex) {
				handleOnStartException(ex);
			}
		});

		WorkHandler<WorkQueueTask>[] workHandlers = new WorkHandler[poolSize];
		for (int i = 0; i < poolSize; i++) {
			workHandlers[i] = new WorkHandler<WorkQueueTask>() {
				@Override
				public void onEvent(WorkQueueTask task) throws Exception {
					task.run();
				}
			};
		}
		this.disruptor.handleEventsWithWorkerPool(workHandlers);

		this.ringBuffer = disruptor.start();
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		shutdown();
		try {
			executor.awaitTermination(timeout, timeUnit);
			disruptor.shutdown();
		} catch (InterruptedException e) {
			return false;
		}
		return true;
	}

	@Override
	public void shutdown() {
		executor.shutdown();
		disruptor.shutdown();
		super.shutdown();
	}

	@Override
	public void forceShutdown() {
		executor.shutdownNow();
		disruptor.halt();
		super.forceShutdown();
	}

	@Override
	public void nervous() {
		if (waitingMood != null) {
			waitingMood.nervous();
		}
	}

	@Override
	public void calm() {
		if (waitingMood != null) {
			waitingMood.calm();
		}
	}

	@Override
	public long remainingSlots() {
		return ringBuffer.remainingCapacity();
	}

	@Override
	protected Task allocateTask() {
		long seqId = ringBuffer.next();
		return ringBuffer.get(seqId).setSequenceId(seqId);
	}

	@Override
	protected Task tryAllocateTask() throws InsufficientCapacityException {
		try {
			long seqId = ringBuffer.tryNext();
			return ringBuffer.get(seqId).setSequenceId(seqId);
		} catch (reactor.jarjar.com.lmax.disruptor.InsufficientCapacityException e) {
			throw InsufficientCapacityException.INSTANCE;
		}
	}

	protected void execute(Task task) {
		ringBuffer.publish(((WorkQueueTask) task).getSequenceId());
	}

	private class WorkQueueTask extends MultiThreadTask {
		private long sequenceId;

		public long getSequenceId() {
			return sequenceId;
		}

		public WorkQueueTask setSequenceId(long sequenceId) {
			this.sequenceId = sequenceId;
			return this;
		}
	}

}
