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

package reactor.bus.batcher.spec;

import reactor.bus.registry.Registries;
import reactor.bus.registry.Registry;
import reactor.bus.batcher.OperationBatcher;
import reactor.bus.selector.Selectors;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.jarjar.com.lmax.disruptor.*;

/**
 * Specification class to create {@link reactor.bus.batcher.OperationBatcher OperationBatchers}.
 *
 * @author Jon Brisbin
 */
public class OperationBatcherSpec<T> implements Supplier<OperationBatcher<T>> {

	private Registry<Consumer<Throwable>> errorConsumers        = Registries.create();
	private boolean                       multiThreadedProducer = false;
	private int                           dataBufferSize        = -1;
	private WaitStrategy                  waitStrategy          = null;
	private Supplier<T> dataSupplier;
	private Consumer<T> consumer;

	/**
	 * Protect against publication of data events from multiple producer threads.
	 *
	 * @return {@literal this}
	 */
	public OperationBatcherSpec<T> multiThreadedProducer() {
		this.multiThreadedProducer = true;
		return this;
	}

	/**
	 * Optimize for highest throughput by assuming only a single thread will be publishing data events into this {@code
	 * Processor}.
	 *
	 * @return {@literal this}
	 */
	public OperationBatcherSpec<T> singleThreadedProducer() {
		this.multiThreadedProducer = false;
		return this;
	}

	/**
	 * How many data objects to pre-allocate in the buffer.
	 *
	 * @param dataBufferSize number of data objects to pre-allocate
	 * @return {@literal this}
	 */
	public OperationBatcherSpec<T> dataBufferSize(int dataBufferSize) {
		this.dataBufferSize = dataBufferSize;
		return this;
	}

	/**
	 * Use the given {@link Supplier} to provide new instances of the data object for pre-allocation.
	 *
	 * @param dataSupplier the {@link Supplier} to provide new data instances
	 * @return {@literal this}
	 */
	public OperationBatcherSpec<T> dataSupplier(Supplier<T> dataSupplier) {
		Assert.isNull(this.dataSupplier, "Data Supplier is already set.");
		this.dataSupplier = dataSupplier;
		return this;
	}

	/**
	 * Set Disruptor's {@link com.lmax.disruptor.WaitStrategy}.
	 *
	 * @param waitStrategy the {@link com.lmax.disruptor.WaitStrategy} to use
	 * @return {@literal this}
	 */
	public OperationBatcherSpec<T> waitStrategy(WaitStrategy waitStrategy) {
		this.waitStrategy = waitStrategy;
		return this;
	}

	/**
	 * Set {@link com.lmax.disruptor.BlockingWaitStrategy} as wait strategy.
	 *
	 * @return {@literal this}
	 */
	public OperationBatcherSpec<T> blockingWaitStrategy() {
		this.waitStrategy = new BlockingWaitStrategy();
		return this;
	}

	/**
	 * Set {@link com.lmax.disruptor.SleepingWaitStrategy} as wait strategy.
	 *
	 * @return {@literal this}
	 */
	public OperationBatcherSpec<T> sleepingWaitStrategy() {
		this.waitStrategy = new SleepingWaitStrategy();
		return this;
	}


	/**
	 * Set {@link com.lmax.disruptor.YieldingWaitStrategy} as wait strategy.
	 *
	 * @return {@literal this}
	 */
	public OperationBatcherSpec<T> yieldingWaitStrategy() {
		this.waitStrategy = new YieldingWaitStrategy();
		return this;
	}

	/**
	 * Set {@link com.lmax.disruptor.BusySpinWaitStrategy} as wait strategy.
	 *
	 * @return {@literal this}
	 */
	public OperationBatcherSpec<T> busySpinWaitStrategy() {
		this.waitStrategy = new BusySpinWaitStrategy();
		return this;
	}

	/**
	 * When data is mutated and published into the {@code Processor}, invoke the given {@link Consumer} and pass the
	 * mutated data.
	 *
	 * @param consumer the mutated event data {@code Consumer}
	 * @return {@literal this}
	 */
	public OperationBatcherSpec<T> consume(Consumer<T> consumer) {
		this.consumer = consumer;
		return this;
	}

	/**
	 * Assign the given {@link Consumer} as an error handler for exceptions of the given type.
	 *
	 * @param type          type of the exception to handle
	 * @param errorConsumer exception {@code Consumer}
	 * @return {@literal this}
	 */
	public OperationBatcherSpec<T> when(Class<? extends Throwable> type, Consumer<Throwable> errorConsumer) {
		errorConsumers.register(Selectors.type(type), errorConsumer);
		return this;
	}


	@Override
	public OperationBatcher<T> get() {
		return new OperationBatcher<T>(dataSupplier,
		                        consumer,
		                        errorConsumers,
		                        waitStrategy,
		                        multiThreadedProducer,
		                        dataBufferSize);
	}

}
