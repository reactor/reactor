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

package reactor.core;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import reactor.fn.Lifecycle;
import reactor.fn.dispatch.*;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@literal Context} is responsible for managing a set of {@link Dispatcher}s for use by all components in a system.
 * If no static {@literal Context} is created, then one must be instantiated with the appropriate configuration.
 *
 * @author Stephane Maldini (smaldini)
 */
public class Context implements Lifecycle {

	private final static int PROCESSORS = Runtime.getRuntime().availableProcessors();

	private final Dispatcher   rootDispatcher;
	private final Dispatcher[] workerDispatchers;
	private final Dispatcher   threadPoolDispatcher;
	private final Dispatcher   syncDispatcher;
	private final AtomicLong nextDispatcherCounter = new AtomicLong(Long.MIN_VALUE);

	private volatile boolean alive = false;

	protected static final ThreadLocal<Context> local = new InheritableThreadLocal<Context>();
	protected static Context self;

	static {
		init();
	}

	/**
	 * Create a {@literal Context} with {@link Dispatcher}s with the given number of threads.
	 *
	 * @param poolSize The number of threads to use when creating root-level {@link Dispatcher}s.
	 */
	public Context(int poolSize) {
		int backlog = Integer.parseInt(System.getProperty("reactor.dispatcher.backlog", "128"));
		int ringbufferThreads = Integer.parseInt(System.getProperty("reactor.max.ringbuffer.threads", "1"));
		int ringbufferbacklog = Integer.parseInt(System.getProperty("reactor.max.ringbuffer.backlog", "512"));
		rootDispatcher = new RingBufferDispatcher(
				"root",
				ringbufferThreads,
				ringbufferbacklog,
				ProducerType.MULTI,
				new BlockingWaitStrategy()
		);

		workerDispatchers = new Dispatcher[poolSize];
		for (int i = 0; i < poolSize; i++) {
			workerDispatchers[i] = new BlockingQueueDispatcher("worker", backlog);
		}

		Dispatcher[] pooledDispatchers = new Dispatcher[poolSize];
		for (int i = 0; i < poolSize; i++) {
			pooledDispatchers[i] = new BlockingQueueDispatcher("worker-pool", backlog);
		}
		threadPoolDispatcher = new ThreadPoolExecutorDispatcher(poolSize * 2, backlog);

		syncDispatcher = new SynchronousDispatcher();

		alive = true;
	}

	@Override
	public synchronized Context destroy() {
		if (alive) {
			for (Dispatcher dispatcher : workerDispatchers) {
				dispatcher.destroy();
			}
			rootDispatcher.destroy();
			threadPoolDispatcher.destroy();
			syncDispatcher.destroy();

			alive = false;
		}

		return this;
	}

	@Override
	public synchronized Context stop() {
		if (alive) {
			for (Dispatcher dispatcher : workerDispatchers) {
				dispatcher.stop();
			}
			rootDispatcher.stop();
			threadPoolDispatcher.stop();
			syncDispatcher.stop();
			alive = false;
		}

		return this;
	}

	@Override
	public synchronized Context start() {
		if (!alive) {
			for (Dispatcher dispatcher : workerDispatchers) {
				dispatcher.start();
			}
			rootDispatcher.start();
			threadPoolDispatcher.start();
			syncDispatcher.start();

			alive = true;
		}

		return this;
	}

	@Override
	public boolean isAlive() {
		return alive;
	}

	/**
	 * Returns a {@link Dispatcher} implementation suitable for same-thread, synchronous task execution.
	 *
	 * @return
	 */
	public static Dispatcher synchronousDispatcher() {
		return self.syncDispatcher;
	}

	/**
	 * Returns the "root" {@link Dispatcher}, which is a high-speed Dispatcher designed for low-latency, non-blocking
	 * tasks.
	 *
	 * @return
	 */
	public static Dispatcher rootDispatcher() {
		return self.rootDispatcher;
	}

	/**
	 * Returns a thread pool-based {@link Dispatcher}, which is a moderately high-speed Dispatcher designed for large
	 * numbers of batch or longer-running tasks.
	 *
	 * @return
	 */
	public static Dispatcher threadPoolDispatcher() {
		return self.threadPoolDispatcher;
	}

	/**
	 * Returns one of a fixed set of "worker" {@link Dispatcher}s. Useful when it is desirable to associate a particular
	 * component with a single {@link Dispatcher}.
	 *
	 * @return
	 */
	public static Dispatcher nextWorkerDispatcher() {
		long l = Math.abs(self.nextDispatcherCounter.incrementAndGet() % (self.workerDispatchers.length));
		return self.workerDispatchers[(int) l];
	}

	/**
	 * Get the current {@link Context} that is a static singleton.
	 *
	 * @return
	 */
	public static Context current() {
		return self;
	}

	/**
	 * Get the thread-local {@link Context}.
	 *
	 * @return
	 */
	public static Context local() {
		return local.get();
	}

	private static void init() {
		boolean init = Boolean.parseBoolean(System.getProperty("reactor.init.auto", "true"));
		if (init) {
			self = new Context(PROCESSORS);
			local.set(self);
			if (null == R.self) {
				R.assignRx(new R());
			}
		}
	}

}
