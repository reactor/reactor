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

import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.fn.Consumer;
import reactor.fn.ConsumerInvoker;
import reactor.fn.support.ConverterAwareConsumerInvoker;
import reactor.support.NamedDaemonThreadFactory;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

/**
 * Implementation of a {@link Dispatcher} that uses a <a href="http://github.com/lmax-exchange/disruptor">Disruptor
 * RingBuffer</a> to queue tasks to execute.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class RingBufferDispatcher implements Dispatcher {

	private static final int DEFAULT_RING_BUFFER_THREADS = Integer.parseInt(System.getProperty("reactor.dispatcher.ringbuffer.threads", "1"));
    private static final int DEFAULT_RING_BUFFER_BACKLOG = Integer.parseInt(System.getProperty("reactor.dispatcher.ringbuffer.backlog", "512"));

	private final    Disruptor<RingBufferTask>  disruptor;
	private volatile ConsumerInvoker            invoker;
	private volatile RingBuffer<RingBufferTask> ringBuffer;

	/**
	 * Creates a new {@literal RingBufferDispatcher} in its default configuration. The dispatcher will be named "ring-buffer". The number of
	 * threads used is determined by the {@code reactor.dispatcher.ringbuffer.threads} system property. If the property is not set, one thread
	 * will be used. The size of the backlog is determined by the {@code reactor dispatcher.ringbuffer.backlog} system property. If the
	 * property is not set, a backlog of 512 will be used. The dispatcher's {@link RingBuffer} will configured to be used with
	 * {@link ProducerType#MULTI multiple producers} and will use a {@link BlockingWaitStrategy blocking wait strategy}.
	 */
	public RingBufferDispatcher() {
		this("ring-buffer", DEFAULT_RING_BUFFER_THREADS, DEFAULT_RING_BUFFER_BACKLOG, ProducerType.MULTI, new BlockingWaitStrategy());
	}

	/**
	 * Creates a new {@literal RingBufferDispatcher} with the given configuration.
	 *
	 * @param name The name of the dispatcher
	 * @param poolSize The size of the thread pool used to remove items when the buffer
	 * @param backlog The backlog size to configuration the ring buffer with
	 * @param producerType The producer type to configure the ring buffer with
	 * @param waitStrategy The wait strategy to configure the ring buffer with
	 */
	@SuppressWarnings({"unchecked"})
	public RingBufferDispatcher(String name,
															int poolSize,
															int backlog,
															ProducerType producerType,
															WaitStrategy waitStrategy) {
		disruptor = new Disruptor<RingBufferTask>(
				new EventFactory<RingBufferTask>() {
					@Override
					public RingBufferTask newInstance() {
						return new RingBufferTask();
					}
				},
				backlog,
				Executors.newFixedThreadPool(poolSize, new NamedDaemonThreadFactory(name + "-dispatcher")),
				producerType,
				waitStrategy
		);

		disruptor.handleEventsWith(new RingBufferTaskHandler());
		disruptor.handleExceptionsWith(
				new ExceptionHandler() {

					@Override
					public void handleEventException(Throwable ex, long sequence, Object event) {
						Logger log = LoggerFactory.getLogger(RingBufferDispatcher.class);
						if (log.isErrorEnabled()) {
							log.error(ex.getMessage(), ex);
						}
						Consumer<Throwable> a;
						if (null != (a = ((Task<?>) event).getErrorConsumer())) {
							a.accept(ex);
						}
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
		invoker = new ConverterAwareConsumerInvoker();
	}

	@Override
	@SuppressWarnings({"unchecked"})
	public <T> Task<T> nextTask() {
		long l = ringBuffer.next();
		RingBufferTask t = ringBuffer.get(l);
		t.setSequenceId(l);
		return (Task<T>) t;
	}

	@Override
	public RingBufferDispatcher destroy() {
		disruptor.shutdown();
		return this;
	}

	@Override
	public RingBufferDispatcher stop() {
		disruptor.halt();
		return this;
	}

	@Override
	public RingBufferDispatcher start() {
		ringBuffer = disruptor.start();
		return this;
	}

	@Override
	public boolean isAlive() {
		return ringBuffer.remainingCapacity() > 0;
	}

	private class RingBufferTask extends Task<Object> {
		private long sequenceId;

		private RingBufferTask setSequenceId(long sequenceId) {
			this.sequenceId = sequenceId;
			return this;
		}

		@Override
		public void submit() {
			ringBuffer.publish(sequenceId);
		}
	}

	private class RingBufferTaskHandler implements EventHandler<RingBufferTask> {
		@Override
		public void onEvent(RingBufferTask t, long sequence, boolean endOfBatch) throws Exception {
			t.execute(invoker);
		}
	}

}
