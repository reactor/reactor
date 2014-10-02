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

package reactor.alloc.spec;

import reactor.alloc.Recyclable;
import reactor.alloc.Reference;
import reactor.alloc.RingBufferAllocator;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.jarjar.com.lmax.disruptor.BlockingWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.EventHandler;
import reactor.jarjar.com.lmax.disruptor.ExceptionHandler;
import reactor.jarjar.com.lmax.disruptor.WaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;
import reactor.util.Assert;

import java.util.concurrent.ExecutorService;

/**
 * Helper class for creating a {@link reactor.alloc.RingBufferAllocator}. Provides an easy way to specify the
 * {@code EventFactory}, {@code EventHandler}, and {@code ExceptionHandler} via Reactor abstractions of {@link
 * reactor.function.Supplier} and {@link Consumer}.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public class RingBufferAllocatorSpec<T extends Recyclable> implements Supplier<RingBufferAllocator<T>> {

	private String name         = "ring-buffer-allocator";
	private int    ringSize     = 1024;
	private int    eventThreads = 1;

	private Supplier<T>            allocator;
	private Consumer<Reference<T>> eventHandler;
	private Consumer<Throwable>    errorHandler;
	private ProducerType           producerType;
	private WaitStrategy           waitStrategy;
	private ExecutorService        executor;

	/**
	 * Name of the {@code RingBufferAllocator}.
	 *
	 * @param name
	 *
	 * @return {@code this}
	 */
	public RingBufferAllocatorSpec<T> name(String name) {
		this.name = name;
		return this;
	}

	/**
	 * Size of the {@code RingBuffer} ring.
	 *
	 * @param ringSize
	 *
	 * @return {@code this}
	 */
	public RingBufferAllocatorSpec<T> ringSize(int ringSize) {
		Assert.isTrue(ringSize > 0, "Ring size must be greater than 0 (zero).");
		this.ringSize = ringSize;
		return this;
	}

	/**
	 * Specify the number of threads the underlying {@link com.lmax.disruptor.RingBuffer} will use.
	 *
	 * @param eventThreads
	 * 		number of threads for event handlers
	 *
	 * @return {@code this}
	 */
	public RingBufferAllocatorSpec<T> eventThreads(int eventThreads) {
		Assert.isTrue(eventThreads > 0, "Threads size must be 1 or greater.");
		this.eventThreads = eventThreads;
		return this;
	}

	/**
	 * The {@link reactor.function.Supplier} to provide the {@code RingBuffer} with the reusable objects that are being
	 * pooled.
	 *
	 * @param allocator
	 *
	 * @return {@code this}
	 */
	public RingBufferAllocatorSpec<T> allocator(Supplier<T> allocator) {
		this.allocator = allocator;
		return this;
	}

	/**
	 * The {@link reactor.function.Consumer} to be invoked with a sequence ID is published.
	 *
	 * @param eventHandler
	 *
	 * @return {@code this}
	 */
	public RingBufferAllocatorSpec<T> eventHandler(Consumer<Reference<T>> eventHandler) {
		this.eventHandler = eventHandler;
		return this;
	}

	/**
	 * The {@link reactor.function.Consumer} to be invoked when an error occurs.
	 *
	 * @param errorHandler
	 *
	 * @return {@code this}
	 */
	public RingBufferAllocatorSpec<T> errorHandler(Consumer<Throwable> errorHandler) {
		this.errorHandler = errorHandler;
		return this;
	}

	/**
	 * Specify single or multi-threaded producer.
	 *
	 * @param producerType
	 *
	 * @return {@code this}
	 */
	public RingBufferAllocatorSpec<T> producerType(ProducerType producerType) {
		this.producerType = producerType;
		return this;
	}

	/**
	 * Specify the {@link com.lmax.disruptor.WaitStrategy} to use (defaults to {@link
	 * com.lmax.disruptor.BlockingWaitStrategy}).
	 *
	 * @param waitStrategy
	 *
	 * @return {@code this}
	 */
	public RingBufferAllocatorSpec<T> waitStrategy(WaitStrategy waitStrategy) {
		this.waitStrategy = waitStrategy;
		return this;
	}

	/**
	 * The {@link java.util.concurrent.ExecutorService} to use inside the {@link com.lmax.disruptor.RingBuffer}.
	 *
	 * @param executor
	 *
	 * @return {@code this}
	 */
	public RingBufferAllocatorSpec<T> executor(ExecutorService executor) {
		this.executor = executor;
		return this;
	}

	@Override
	public RingBufferAllocator<T> get() {
		Assert.notNull(allocator, "Object Supplier (allocator) cannot be null.");

		if(null == producerType) {
			producerType = ProducerType.MULTI;
		}
		if(null == waitStrategy) {
			waitStrategy = new BlockingWaitStrategy();
		}

		RingBufferAllocator<T> alloc = new RingBufferAllocator<T>(
				name,
				ringSize,
				allocator,
				eventThreads,
				(null != eventHandler ? new EventHandler<Reference<T>>() {
					@Override
					public void onEvent(Reference<T> ref, long sequence, boolean endOfBatch) throws Exception {
						eventHandler.accept(ref);
						if(ref.getReferenceCount() > 0) {
							ref.release();
						}
					}
				} : null),
				(null != errorHandler ? new ExceptionHandler() {
					@Override
					public void handleEventException(Throwable ex, long sequence, Object event) {
						errorHandler.accept(ex);
					}

					@Override
					public void handleOnStartException(Throwable ex) {
						errorHandler.accept(ex);
					}

					@Override
					public void handleOnShutdownException(Throwable ex) {
						errorHandler.accept(ex);
					}
				} : null),
				producerType,
				waitStrategy,
				executor
		);
		// auto-start
		alloc.start();

		return alloc;
	}

}
