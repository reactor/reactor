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

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.event.Event;
import reactor.support.NamedDaemonThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Implementation of a {@link Dispatcher} that uses a {@link RingBuffer} to queue tasks to execute.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class RingBufferDispatcher extends BaseLifecycleDispatcher {

	private static final int DEFAULT_BUFFER_SIZE = 1024;

	private final ExecutorService               executor;
	private final Disruptor<RingBufferTask<?>>  disruptor;
	private final RingBuffer<RingBufferTask<?>> ringBuffer;

	/**
	 * Creates a new {@code RingBufferDispatcher} with the given {@code name}. It will use a RingBuffer with 1024 slots,
	 * configured with a producer type of {@link ProducerType#MULTI MULTI} and a {@link BlockingWaitStrategy blocking wait
	 * strategy}.
	 *
	 * @param name The name of the dispatcher.
	 */
	public RingBufferDispatcher(String name) {
		this(name, DEFAULT_BUFFER_SIZE, ProducerType.MULTI, new BlockingWaitStrategy());
	}

	/**
	 * Creates a new {@literal RingBufferDispatcher} with the given {@code name}. It will use a {@link RingBuffer} with
	 * {@code bufferSize} slots, configured with the given {@code producerType} and {@code waitStrategy}.
	 *
	 * @param name         The name of the dispatcher
	 * @param bufferSize   The size to configure the ring buffer with
	 * @param producerType The producer type to configure the ring buffer with
	 * @param waitStrategy The wait strategy to configure the ring buffer with
	 */
	@SuppressWarnings({"unchecked"})
	public RingBufferDispatcher(String name,
															int bufferSize,
															ProducerType producerType,
															WaitStrategy waitStrategy) {
		this.executor = Executors.newSingleThreadExecutor(new NamedDaemonThreadFactory(name + "-ringbuffer"));

		this.disruptor = new Disruptor<RingBufferTask<?>>(
				new EventFactory<RingBufferTask<?>>() {
					@SuppressWarnings("rawtypes")
					@Override
					public RingBufferTask<?> newInstance() {
						return new RingBufferTask();
					}
				},
				bufferSize,
				executor,
				producerType,
				waitStrategy
		);
		// Exceptions are handled by the errorConsumer
		disruptor.handleExceptionsWith(
				new ExceptionHandler() {
					@Override
					public void handleEventException(Throwable ex, long sequence, Object event) {
						// Handled by Task.execute
					}

					@Override
					public void handleOnStartException(Throwable ex) {
						Logger log = LoggerFactory.getLogger(RingBufferDispatcher.class);
						if (log.isErrorEnabled()) {
							log.error(ex.getMessage(), ex);
						}
					}

					@Override
					public void handleOnShutdownException(Throwable ex) {
						Logger log = LoggerFactory.getLogger(RingBufferDispatcher.class);
						if (log.isErrorEnabled()) {
							log.error(ex.getMessage(), ex);
						}
					}
				}
		);
		disruptor.handleEventsWith(new RingBufferTaskHandler());

		ringBuffer = disruptor.start();
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

	@SuppressWarnings("unchecked")
	@Override
	protected <E extends Event<?>> Task<E> createTask() {
		long l = ringBuffer.next();
		RingBufferTask<?> t = ringBuffer.get(l);
		t.setSequenceId(l);
		return (Task<E>) t;
	}

	private class RingBufferTask<E extends Event<?>> extends Task<E> {
		private long sequenceId;

		private RingBufferTask<E> setSequenceId(long sequenceId) {
			this.sequenceId = sequenceId;
			return this;
		}

		@Override
		public void submit() {
			ringBuffer.publish(sequenceId);
		}
	}

	private class RingBufferTaskHandler implements EventHandler<RingBufferTask<?>> {
		@Override
		public void onEvent(RingBufferTask<?> t, long sequence, boolean endOfBatch) throws Exception {
			t.execute();
		}
	}

}
