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

package reactor.bus.processor;

import reactor.bus.registry.Registration;
import reactor.bus.registry.Registry;
import reactor.core.support.Assert;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.fn.batch.BatchConsumer;
import reactor.jarjar.com.lmax.disruptor.*;
import reactor.jarjar.com.lmax.disruptor.dsl.Disruptor;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A {@code Processor} is a highly-efficient data processor that is backed by an <a
 * href="https://github.com/LMAX-Exchange/disruptor">LMAX Disruptor RingBuffer</a>.
 * <p>
 * Rather than dealing with dynamic {@link Consumer} registration and event routing, the {@code Processor} gains its
 * extreme efficiency from pre-allocating the data objects (for which you pass a {@link Supplier} whose job is to
 * provide new instances of the data class on startup) and by baking-in a {@code Consumer} for published events.
 * <p>
 * The {@code Processor} can be used in two different modes: single-operation or batch. For single operations, use the
 * {@link #prepare()} method to allocate an object from the {@link RingBuffer}. An {@link Operation} is also a {@link
 * Supplier}, so call {@link reactor.fn.Supplier#get()} to get access to the data object. One can then update the
 * members on the data object and, by calling {@link Operation#commit()}, publish the event into
 * the {@link RingBuffer} to be handled by the {@link Consumer}.
 * <p>
 * To operate on the {@code Processor} in batch mode, first set a {@link BatchConsumer} as the {@link Consumer} of
 * events. This interface provides two additional methods, {@link reactor.fn.batch.BatchConsumer#start()}, which
 * is invoked before the batch starts, and {@link reactor.fn.batch.BatchConsumer#end()}, which is invoked when
 * the
 * batch is submitted. The {@link BatchConsumer} will work for either single-operation mode or batch mode, but only a
 * {@link BatchConsumer} will be able to recognize the start and end of a batch.
 *
 * @author Jon Brisbin
 * @see <a href="https://github.com/LMAX-Exchange/disruptor">https://github.com/LMAX-Exchange/disruptor</a>
 */
public class Processor<T> implements Supplier<Operation<T>> {

	private final int                      opsBufferSize;
	private final ExecutorService          executor;
	private final Disruptor<Operation<T>>  disruptor;
	private final RingBuffer<Operation<T>> ringBuffer;

	@SuppressWarnings("unchecked")
	public Processor(@Nonnull final Supplier<T> dataSupplier,
	                 @Nonnull final Consumer<T> consumer,
	                 @Nonnull Registry<Consumer<Throwable>> errorConsumers,
	                 WaitStrategy waitStrategy,
	                 boolean multiThreadedProducer,
	                 int opsBufferSize) {
		Assert.notNull(dataSupplier, "Data Supplier cannot be null.");
		Assert.notNull(consumer, "Consumer cannot be null.");
		Assert.notNull(errorConsumers, "Error Consumers Registry cannot be null.");

		executor = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("processor"));

		if (opsBufferSize < 1) {
			this.opsBufferSize = 256 * Runtime.getRuntime().availableProcessors();
		} else {
			this.opsBufferSize = opsBufferSize;
		}

		disruptor = new Disruptor<Operation<T>>(
				new EventFactory<Operation<T>>() {
					@SuppressWarnings("rawtypes")
					@Override
					public Operation<T> newInstance() {
						return new Operation<T>(dataSupplier.get()) {
							@Override
							public void commit() {
								ringBuffer.publish(id);
							}
						};
					}
				},
				this.opsBufferSize,
				executor,
				(multiThreadedProducer ? ProducerType.MULTI : ProducerType.SINGLE),
				(waitStrategy != null ? waitStrategy : new BlockingWaitStrategy())
		);

		disruptor.handleExceptionsWith(new ConsumerExceptionHandler(errorConsumers));
		disruptor.handleEventsWith(new ConsumerEventHandler<T>(consumer));

		ringBuffer = disruptor.start();
	}

	/**
	 * Shutdown this {@code Processor} by shutting down the thread pool.
	 */
	public void shutdown() {
		executor.shutdown();
		disruptor.shutdown();
	}

	/**
	 * Prepare an {@link Operation} by allocating it from the buffer and preparing it to be submitted once {@link
	 * Operation#commit()} is invoked.
	 *
	 * @return an {@link Operation} instance allocated from the buffer
	 */
	public Operation<T> prepare() {
		long seqId = ringBuffer.next();
		Operation<T> op = ringBuffer.get(seqId);
		op.setId(seqId);

		return op;
	}

	/**
	 * Commit a list of {@link Operation Operations} by doing a batch publish.
	 *
	 * @param ops the {@code Operations} to commit
	 * @return {@literal this}
	 */
	public Processor<T> commit(List<Operation<T>> ops) {
		if (null == ops || ops.isEmpty()) {
			return this;
		}

		Operation<T> first = ops.get(0);
		Operation<T> last = ops.get(ops.size() - 1);

		long firstSeqId = first.id;
		long lastSeqId = last.id;

		if (lastSeqId > firstSeqId) {
			ringBuffer.publish(firstSeqId, lastSeqId);
		} else {
			ringBuffer.publish(firstSeqId);
		}

		return this;
	}

	/**
	 * If the {@link Consumer} set in the spec is a {@link BatchConsumer}, then the start method will be invoked before
	 * the batch is published, then all the events of the batch are published to the consumer, then the batch end is
	 * published.
	 * <p>
	 * The {@link Consumer} passed here is a mutator. Rather than accessing the data object directly, as one would do
	 * with a {@link #prepare()} call, pass a {@link Consumer} here that will accept the allocated data object which one
	 * can update with the appropriate data. Note that this is not an event handler. The event handler {@link Consumer}
	 * is
	 * specified in the spec (which is passed into the {@code Processor} constructor).
	 *
	 * @param size    size of the batch
	 * @param mutator a {@link Consumer} that mutates the data object before it is published as an event and handled by the
	 *                event {@link Consumer}
	 * @return {@literal this}
	 */
	public Processor<T> batch(int size, Consumer<T> mutator) {
		long start = -1;
		long end = 0;
		for (int i = 0; i < size; i++) {
			if (i > 0 && i % opsBufferSize == 0) {
				// Automatically flush when buffer is full
				ringBuffer.publish(start, end);
				start = -1;
			}
			long l = ringBuffer.next();
			if (start < 0) {
				start = l;
			}
			end = l;
			mutator.accept(ringBuffer.get(end).get());
		}
		ringBuffer.publish(start, end);

		return this;
	}

	@Override
	public Operation<T> get() {
		return prepare();
	}

	private static class ConsumerEventHandler<T> implements EventHandler<Operation<T>>, LifecycleAware {
		final Consumer<T> consumer;
		final boolean     isBatchConsumer;

		private ConsumerEventHandler(Consumer<T> consumer) {
			this.consumer = consumer;
			this.isBatchConsumer = consumer instanceof BatchConsumer;
		}

		@Override
		public void onStart() {
			if (isBatchConsumer) {
				((BatchConsumer) consumer).start();
			}
		}

		@Override
		public void onShutdown() {
			if (isBatchConsumer) {
				((BatchConsumer) consumer).end();
			}
		}

		@Override
		public void onEvent(Operation<T> op, long sequence, boolean endOfBatch) throws Exception {
			consumer.accept(op.get());
		}
	}

	private static class ConsumerExceptionHandler implements ExceptionHandler {
		final Registry<Consumer<Throwable>> errorConsumers;

		private ConsumerExceptionHandler(Registry<Consumer<Throwable>> errorConsumers) {
			this.errorConsumers = errorConsumers;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void handleEventException(Throwable ex, long sequence, Object event) {
			for (Registration<? extends Consumer<? extends Throwable>> reg : errorConsumers.select(ex.getClass())) {
				((Consumer<Throwable>) reg.getObject()).accept(ex);
			}
		}

		@Override
		public void handleOnStartException(Throwable ex) {
			handleEventException(ex, -1, null);
		}

		@Override
		public void handleOnShutdownException(Throwable ex) {
			handleEventException(ex, -1, null);
		}
	}

}
