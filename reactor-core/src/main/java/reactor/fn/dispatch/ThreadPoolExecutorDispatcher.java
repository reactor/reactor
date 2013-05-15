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

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.fn.*;
import reactor.support.NamedDaemonThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * A {@code Dispatcher} that uses a {@link ThreadPoolExecutor} with an unbounded queue to execute {@link Task Tasks}.
 *
 * @author Andy Wilkinson
 */
public final class ThreadPoolExecutorDispatcher implements Dispatcher {

	private static final Logger LOG = LoggerFactory.getLogger(ThreadPoolExecutorDispatcher.class);
	private final RingBuffer<RingBufferTask> ringBuffer;
	private final Disruptor<RingBufferTask>  disruptor;
	private final ExecutorService            executor;
	private volatile ConsumerInvoker invoker = new ConverterAwareConsumerInvoker();

	@SuppressWarnings("unchecked")
	public ThreadPoolExecutorDispatcher(int poolSize, int backlog) {
		executor = Executors.newFixedThreadPool(poolSize + 1, new NamedDaemonThreadFactory("thread-pool-executor"));

		disruptor = new Disruptor<RingBufferTask>(
				new EventFactory<RingBufferTask>() {
					@Override
					public RingBufferTask newInstance() {
						return new RingBufferTask();
					}
				},
				backlog,
				executor
		);
		disruptor.handleEventsWith(new EventHandler<RingBufferTask>() {
			@Override
			public void onEvent(RingBufferTask task, long sequence, boolean endOfBatch) throws Exception {
				task.reset();
			}
		});

		ringBuffer = disruptor.start();
	}

	@Override
	public ThreadPoolExecutorDispatcher destroy() {
		disruptor.halt();
		return this;
	}

	@Override
	public ThreadPoolExecutorDispatcher stop() {
		disruptor.shutdown();
		executor.shutdown();
		return this;
	}

	@Override
	public ThreadPoolExecutorDispatcher start() {
		return this;
	}

	@Override
	public boolean isAlive() {
		return executor.isShutdown();
	}

	@Override
	public ConsumerInvoker getConsumerInvoker() {
		return this.invoker;
	}

	@Override
	public ThreadPoolExecutorDispatcher setConsumerInvoker(ConsumerInvoker consumerInvoker) {
		this.invoker = consumerInvoker;
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> Task<T> nextTask() {
		long l = ringBuffer.next();
		RingBufferTask t = ringBuffer.get(l);
		t.setSequenceId(l);
		return (Task<T>) t;
	}

	private class RingBufferTask extends Task<Object> implements Runnable {
		private long sequenceId;

		private RingBufferTask setSequenceId(long sequenceId) {
			this.sequenceId = sequenceId;
			return this;
		}

		@Override
		public void submit() {
			executor.submit(this);
		}

		@Override
		public void run() {
			try {
				for (Registration<? extends Consumer<? extends Event<?>>> reg : getConsumerRegistry().select(getKey())) {
					if (reg.isCancelled() || reg.isPaused()) {
						continue;
					}
					invoker.invoke(reg.getObject(), getConverter(), Void.TYPE, getEvent());
					if (reg.isCancelAfterUse()) {
						reg.cancel();
					}
				}
				if (null != getCompletionConsumer()) {
					invoker.invoke(getCompletionConsumer(), getConverter(), Void.TYPE, getEvent());
				}
			} catch (Throwable x) {
				LOG.error(x.getMessage(), x);
				if (null != getErrorConsumer()) {
					getErrorConsumer().accept(x);
				}
			} finally {
				ringBuffer.publish(sequenceId);
			}
		}
	}

}
