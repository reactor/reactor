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

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.alloc.Reference;
import reactor.core.alloc.RingBufferAllocator;
import reactor.core.alloc.spec.RingBufferAllocatorSpec;
import reactor.function.Consumer;
import reactor.function.Supplier;
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
public final class RingBufferDispatcher extends AbstractRunnableTaskDispatcher {

	private static final int DEFAULT_BUFFER_SIZE = 1024;

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final ExecutorService                     executor;
	private final RingBufferAllocator<RingBufferTask> tasks;

	/**
	 * Creates a new {@code RingBufferDispatcher} with the given {@code name}. It will use a RingBuffer with 1024 slots,
	 * configured with a producer type of {@link ProducerType#MULTI MULTI} and a {@link BlockingWaitStrategy blocking
	 * wait
	 * strategy}.
	 *
	 * @param name
	 * 		The name of the dispatcher.
	 */
	public RingBufferDispatcher(String name) {
		this(name, DEFAULT_BUFFER_SIZE, ProducerType.MULTI, new BlockingWaitStrategy());
	}

	/**
	 * Creates a new {@literal RingBufferDispatcher} with the given {@code name}. It will use a {@link RingBuffer} with
	 * {@code bufferSize} slots, configured with the given {@code producerType} and {@code waitStrategy}.
	 *
	 * @param name
	 * 		The name of the dispatcher
	 * @param bufferSize
	 * 		The size to configure the ring buffer with
	 * @param producerType
	 * 		The producer type to configure the ring buffer with
	 * @param waitStrategy
	 * 		The wait strategy to configure the ring buffer with
	 */
	@SuppressWarnings({"unchecked"})
	public RingBufferDispatcher(String name,
	                            int bufferSize,
	                            ProducerType producerType,
	                            WaitStrategy waitStrategy) {
		super(bufferSize, null);
		this.executor = Executors.newSingleThreadExecutor(new NamedDaemonThreadFactory(name, getContext()));
		this.tasks = new RingBufferAllocatorSpec<RingBufferTask>()
				.name(name)
				.ringSize(bufferSize)
				.executor(executor)
				.allocator(new Supplier<RingBufferTask>() {
					@Override
					public RingBufferTask get() {
						return new RingBufferTask();
					}
				})
				.eventHandler(new Consumer<Reference<RingBufferTask>>() {
					@Override
					public void accept(Reference<RingBufferTask> ref) {
						ref.get().run();
						ref.release();
					}
				})
				.errorHandler(new Consumer<Throwable>() {
					@Override
					public void accept(Throwable throwable) {
						log.error(throwable.getMessage(), throwable);
					}
				})
				.producerType(producerType)
				.waitStrategy(waitStrategy)
				.get();
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		try {
			executor.awaitTermination(timeout, timeUnit);
			tasks.awaitAndShutdown(timeout, timeUnit);
		} catch(InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
		return true;
	}

	@Override
	public void shutdown() {
		executor.shutdown();
		tasks.shutdown();
		super.shutdown();
	}

	@Override
	public void halt() {
		executor.shutdownNow();
		tasks.halt();
		super.halt();
	}

	@Override
	protected RingBufferTask allocateTask() {
		Reference<RingBufferTask> ref = tasks.allocate();
		ref.get().setReference(ref);
		return ref.get();
	}

	@Override
	protected void submit(RunnableTask task) {
		((RingBufferTask)task).getReference().release();
	}

	private class RingBufferTask extends RunnableTask {
		private Reference<RingBufferTask> reference;

		public Reference<RingBufferTask> getReference() {
			return reference;
		}

		public void setReference(Reference<RingBufferTask> reference) {
			this.reference = reference;
		}
	}

}
