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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import reactor.fn.ConsumerInvoker;
import reactor.fn.support.ConverterAwareConsumerInvoker;
import reactor.support.NamedDaemonThreadFactory;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * A {@code Dispatcher} that uses a {@link ThreadPoolExecutor} with an unbounded queue to execute {@link Task Tasks}.
 *
 * @author Andy Wilkinson
 */
public final class ThreadPoolExecutorDispatcher implements Dispatcher {
	
	private static final int DEFAULT_POOL_SIZE = Runtime.getRuntime().availableProcessors();
	
	private static final int DEFAULT_BACKLOG = Integer.parseInt(System.getProperty("reactor.dispatcher.backlog", "128"));


	private final Disruptor<RingBufferTask>  disruptor;
	private final ExecutorService            executor;
	
	private volatile ConsumerInvoker            invoker = new ConverterAwareConsumerInvoker();	
	private volatile RingBuffer<RingBufferTask> ringBuffer;
	
	/**
	 * Creates a new {@literal ThreadPoolExecutorDispatcher} that will use the default backlog,
	 * as configured by the {@code reactor.dispatcher.backlog} system property (if the property is not set,
	 * a backlog of 128 is used), and the default pool size: the number of available processors.
	 * 
	 * @see Runtime#availableProcessors()
	 */
	public ThreadPoolExecutorDispatcher() {
		this(DEFAULT_POOL_SIZE, DEFAULT_BACKLOG);
	}

	@SuppressWarnings("unchecked")
	/**
	 * Creates a new {@literal ThreadPoolExecutorDispatcher} with the given {@literal poolSize} and {@literal backlog}.
	 * 
	 * @param poolSize the pool size
	 * @param backlog the backlog size
	 *
	 */
	public ThreadPoolExecutorDispatcher(int poolSize, int backlog) {
		executor = Executors.newFixedThreadPool(poolSize, new NamedDaemonThreadFactory("thread-pool-executor-dispatcher"));

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
		ringBuffer = disruptor.start();
		return this;
	}

	@Override
	public boolean isAlive() {
		return executor.isShutdown();
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
				execute(invoker);
			}
			finally {
				ringBuffer.publish(sequenceId);
			}
		}
	}

}
