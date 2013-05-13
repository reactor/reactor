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

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.fn.*;
import reactor.support.NamedDaemonThreadFactory;

import java.util.concurrent.Executors;

/**
 * Implementation of a {@link Dispatcher} that uses a <a href="http://github.com/lmax-exchange/disruptor">Disruptor
 * RingBuffer</a> to queue tasks to execute.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class RingBufferDispatcher implements Dispatcher {

	private final static Logger LOG = LoggerFactory.getLogger(RingBufferDispatcher.class);
	private final    RingBuffer<RingBufferTask> ringBuffer;
	private final    Disruptor<RingBufferTask>  disruptor;
	private volatile ConsumerInvoker            invoker;

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
						LOG.error(ex.getMessage(), ex);
						Consumer<Throwable> a;
						if (null != (a = ((Task<?>) event).getErrorConsumer())) {
							a.accept(ex);
						}
					}

					@Override
					public void handleOnStartException(Throwable ex) {
						LOG.error(ex.getMessage(), ex);
					}

					@Override
					public void handleOnShutdownException(Throwable ex) {
						LOG.error(ex.getMessage(), ex);
					}
				}
		);
		ringBuffer = disruptor.start();

		invoker = new ConverterAwareConsumerInvoker();
	}

	@Override
	public ConsumerInvoker getConsumerInvoker() {
		return invoker;
	}

	@Override
	public RingBufferDispatcher setConsumerInvoker(ConsumerInvoker consumerInvoker) {
		this.invoker = consumerInvoker;
		return this;
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
	public Lifecycle destroy() {
		disruptor.shutdown();
		return this;
	}

	@Override
	public Lifecycle stop() {
		disruptor.halt();
		return this;
	}

	@Override
	public Lifecycle start() {
		disruptor.start();
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
		@SuppressWarnings({"unchecked"})
		public void onEvent(RingBufferTask t, long sequence, boolean endOfBatch) throws Exception {
			for (Registration<? extends Consumer<? extends Event<?>>> reg : t.getConsumerRegistry().select(t.getKey())) {
				if (reg.isCancelled() || reg.isPaused()) {
					continue;
				}
				if (null != reg.getSelector().getHeaderResolver()) {
					t.getEvent().getHeaders().setAll(reg.getSelector().getHeaderResolver().resolve(t.getKey()));
				}
				invoker.invoke(reg.getObject(), t.getConverter(), Void.TYPE, t.getEvent());
				if (reg.isCancelAfterUse()) {
					reg.cancel();
				}
			}
			if (null != t.getCompletionConsumer()) {
				invoker.invoke(t.getCompletionConsumer(), t.getConverter(), Void.TYPE, t.getEvent());
			}
		}
	}

}
