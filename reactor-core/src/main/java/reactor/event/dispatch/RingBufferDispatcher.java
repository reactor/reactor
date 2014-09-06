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
import reactor.event.dispatch.wait.WaitingMood;
import reactor.function.Consumer;
import reactor.jarjar.com.lmax.disruptor.*;
import reactor.jarjar.com.lmax.disruptor.dsl.Disruptor;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;
import reactor.support.NamedDaemonThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of a {@link Dispatcher} that uses a {@link RingBuffer} to queue tasks to execute.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class RingBufferDispatcher extends SingleThreadDispatcher implements WaitingMood {

	private static final int DEFAULT_BUFFER_SIZE = 1024;

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final ExecutorService            executor;
	private final Disruptor<RingBufferTask>  disruptor;
	private final RingBuffer<RingBufferTask> ringBuffer;
	private final WaitingMood                waitingMood;

	/**
	 * Creates a new {@code RingBufferDispatcher} with the given {@code name}. It will use a RingBuffer with 1024 slots,
	 * configured with a producer type of {@link ProducerType#MULTI MULTI} and a {@link BlockingWaitStrategy blocking
	 * wait
	 * strategy}.
	 *
	 * @param name The name of the dispatcher.
	 */
	public RingBufferDispatcher(String name) {
		this(name, DEFAULT_BUFFER_SIZE);
	}

	/**
	 * Creates a new {@code RingBufferDispatcher} with the given {@code name} and {@param bufferSize},
	 * configured with a producer type of {@link ProducerType#MULTI MULTI} and a {@link BlockingWaitStrategy blocking
	 * wait
	 * strategy}.
	 *
	 * @param name       The name of the dispatcher
	 * @param bufferSize The size to configure the ring buffer with
	 */
	@SuppressWarnings({"unchecked"})
	public RingBufferDispatcher(String name,
	                            int bufferSize
	) {
		this(name, bufferSize, null);
	}

	/**
	 * Creates a new {@literal RingBufferDispatcher} with the given {@code name}. It will use a {@link RingBuffer} with
	 * {@code bufferSize} slots, configured with a producer type of {@link ProducerType#MULTI MULTI}
	 * and a {@link BlockingWaitStrategy blocking wait. A given @param uncaughtExceptionHandler} will catch anything not
	 * handled e.g. by the owning {@link reactor.core.Reactor} or {@link reactor.rx.Stream}.
	 *
	 * @param name                     The name of the dispatcher
	 * @param bufferSize               The size to configure the ring buffer with
	 * @param uncaughtExceptionHandler The last resort exception handler
	 */
	@SuppressWarnings({"unchecked"})
	public RingBufferDispatcher(String name,
	                            int bufferSize,
	                            final Consumer<Throwable> uncaughtExceptionHandler) {
		this(name, bufferSize, uncaughtExceptionHandler, ProducerType.MULTI, new BlockingWaitStrategy());

	}

	/**
	 * Creates a new {@literal RingBufferDispatcher} with the given {@code name}. It will use a {@link RingBuffer} with
	 * {@code bufferSize} slots, configured with the given {@code producerType}, {@param uncaughtExceptionHandler}
	 * and {@code waitStrategy}. A null {@param uncaughtExceptionHandler} will make this dispatcher logging such
	 * exceptions.
	 *
	 * @param name                     The name of the dispatcher
	 * @param bufferSize               The size to configure the ring buffer with
	 * @param producerType             The producer type to configure the ring buffer with
	 * @param waitStrategy             The wait strategy to configure the ring buffer with
	 * @param uncaughtExceptionHandler The last resort exception handler
	 */
	@SuppressWarnings({"unchecked"})
	public RingBufferDispatcher(String name,
	                            int bufferSize,
	                            final Consumer<Throwable> uncaughtExceptionHandler,
	                            ProducerType producerType,
	                            WaitStrategy waitStrategy) {
		super(bufferSize);

		if (WaitingMood.class.isAssignableFrom(waitStrategy.getClass())) {
			this.waitingMood = (WaitingMood) waitStrategy;
		} else {
			this.waitingMood = null;
		}

		this.executor = Executors.newSingleThreadExecutor(new NamedDaemonThreadFactory(name, getContext()));
		this.disruptor = new Disruptor<RingBufferTask>(
				new EventFactory<RingBufferTask>() {
					@Override
					public RingBufferTask newInstance() {
						return new RingBufferTask();
					}
				},
				bufferSize,
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
		this.disruptor.handleEventsWith(new EventHandler<RingBufferTask>() {
			@Override
			public void onEvent(RingBufferTask task, long sequence, boolean endOfBatch) throws Exception {
				task.run();
			}
		});

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
	public void halt() {
		executor.shutdownNow();
		disruptor.halt();
		super.halt();
	}

	@Override
	public long remainingSlots() {
		return ringBuffer.remainingCapacity();
	}

	@Override
	public void nervous() {
		if (waitingMood != null) {
			execute(new Runnable() {
				@Override
				public void run() {
					waitingMood.nervous();
				}
			});
		}
	}

	@Override
	public void calm() {
		if (waitingMood != null) {
			execute(new Runnable() {
				@Override
				public void run() {
					waitingMood.calm();
				}
			});

		}
	}

	@Override
	protected Task allocateTask() {
		long seqId = ringBuffer.next();
		return ringBuffer.get(seqId).setSequenceId(seqId);
	}

	protected void execute(Task task) {
		ringBuffer.publish(((RingBufferTask) task).getSequenceId());
	}

	private class RingBufferTask extends SingleThreadTask {
		private long sequenceId;

		public long getSequenceId() {
			return sequenceId;
		}

		public RingBufferTask setSequenceId(long sequenceId) {
			this.sequenceId = sequenceId;
			return this;
		}
	}

}
