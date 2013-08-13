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

package reactor.core.processor;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.function.batch.BatchConsumer;
import reactor.support.NamedDaemonThreadFactory;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A {@code Processor} is a highly-efficient data processor that is backed by an <a
 * href="https://github.com/LMAX-Exchange/disruptor">LMAX Disruptor RingBuffer</a>.
 * <p/>
 * Rather than dealing with dynamic {@link Consumer} registration and event routing, the {@code Processor} gains its
 * extreme efficiency from pre-allocating the data objects (for which you pass a {@link Supplier} whose job is to
 * provide new instances of the data class on startup) and by baking-in a {@code Consumer} for published events.
 * <p/>
 * The {@code Processor} can be used in two different modes: single-operation or batch. For single operations, use the
 * {@link #prepare()} method to allocate an object from the {@link RingBuffer}. An {@link Operation} is also a {@link
 * Supplier}, so call {@link reactor.function.Supplier#get()} to get access to the data object. One can then update the
 * members on the data object and, by calling {@link reactor.core.processor.Operation#commit()}, publish the event into
 * the {@link RingBuffer} to be handled by the {@link Consumer}.
 * <p/>
 * To operate on the {@code Processor} in batch mode, first set a {@link BatchConsumer} as the {@link Consumer} of
 * events. This interface provides two additional methods, {@link reactor.function.batch.BatchConsumer#start()}, which
 * is invoked before the batch starts, and {@link reactor.function.batch.BatchConsumer#end()}, which is invoked when the
 * batch is submitted. The {@link BatchConsumer} will work for either single-operation mode or batch mode, but only a
 * {@link BatchConsumer} will be able to recognize the start and end of a batch.
 *
 * @author Jon Brisbin
 * @see {@link https://github.com/LMAX-Exchange/disruptor}
 */
public class Processor<T> implements Supplier<Operation<T>> {

	private final int                      opsBufferSize;
	private final ExecutorService          executor;
	private final Disruptor<Operation<T>>  disruptor;
	private final RingBuffer<Operation<T>> ringBuffer;

	@SuppressWarnings("unchecked")
	public Processor(@Nonnull final Supplier<T> dataSupplier,
									 @Nonnull final List<Consumer<T>> consumers,
									 @Nonnull Registry<Consumer<Throwable>> errorConsumers,
									 boolean multiThreadedProducer,
									 int opsBufferSize) {
		Assert.notNull(dataSupplier, "Data Supplier cannot be null.");
		Assert.notNull(errorConsumers, "Error Consumers Registry cannot be null.");

		executor = Executors.newSingleThreadExecutor(new NamedDaemonThreadFactory("processor"));

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
				new YieldingWaitStrategy()
		);
		for (Consumer<T> consumer : consumers) {
			disruptor.handleEventsWith(new ConsumerEventHandler(consumer));
		}
		disruptor.handleExceptionsWith(new ConsumerExceptionHandler(errorConsumers));

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
	 * reactor.core.processor.Operation#commit()} is invoked.
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
	 * If the {@link Consumer} set in the spec is a {@link BatchConsumer}, then the start method will be invoked before the
	 * batch is published, then all the events of the batch are published to the consumer, then the batch end is
	 * published.
	 * <p/>
	 * The {@link Consumer} passed here is a mutator. Rather than accessing the data object directly, as one would do with
	 * a {@link #prepare()} call, pass a {@link Consumer} here that will accept the allocated data object which one can
	 * update with the appropriate data. Note that this is not an event handler. The event handler {@link Consumer} is
	 * specified in the spec (which is passed into the {@code Processor} constructor).
	 *
	 * @param size    size of the batch
	 * @param mutator a {@link Consumer} that mutates the data object before it is published as an event and handled by the
	 *                event {@link Consumer}
	 * @return {@literal this}
	 */
	public Processor<T> batch(int size, Consumer<T> mutator) {
		long seqId = 0;
		for (int i = 0; i < size; i++) {
			seqId = ringBuffer.next();
			mutator.accept(ringBuffer.get(seqId).get());
		}
		ringBuffer.publish(seqId - size, seqId);

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
