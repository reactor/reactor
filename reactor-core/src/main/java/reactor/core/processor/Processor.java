package reactor.core.processor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.event.registry.CachingRegistry;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.support.NamedDaemonThreadFactory;
import reactor.util.Assert;

/**
 * @author Jon Brisbin
 */
public class Processor<T> {

	private static final Logger LOG = LoggerFactory.getLogger(Processor.class);

	private final ReentrantLock                           consumersLock  = new ReentrantLock();
	private final List<Consumer<T>>                       consumers      = new ArrayList<Consumer<T>>();
	private final Registry<Consumer<? extends Throwable>> errorConsumers = new CachingRegistry<Consumer<? extends Throwable>>();

	private final int                                         opsBufferSize;
	private final Disruptor<Operation<T>>                     disruptor;
	private final com.lmax.disruptor.RingBuffer<Operation<T>> ringBuffer;

	@SuppressWarnings("unchecked")
	public Processor(@Nullable Executor executor,
	                 @Nonnull final Supplier<T> dataSupplier,
	                 boolean multiThreadedProducer,
	                 int opsBufferSize) {
		Assert.notNull(dataSupplier, "Data Supplier cannot be null.");

		if(null == executor) {
			executor = Executors.newSingleThreadExecutor(new NamedDaemonThreadFactory("processor"));
		}
		if(opsBufferSize < 1) {
			this.opsBufferSize = 256 * Runtime.getRuntime().availableProcessors();
		} else {
			this.opsBufferSize = opsBufferSize;
		}

		this.disruptor = new Disruptor<Operation<T>>(
				new EventFactory<Operation<T>>() {
					@SuppressWarnings("rawtypes")
					@Override
					public Operation<T> newInstance() {
						return new Operation<T>(dataSupplier.get()) {
							@Override public void commit() {
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
		this.disruptor.handleEventsWith(new EventHandler<Operation<T>>() {
			@Override public void onEvent(Operation<T> op, long sequence, boolean endOfBatch) throws Exception {
				consumersLock.lock();
				try {
					int size = consumers.size();
					for(int i = 0; i < size; i++) {
						consumers.get(i).accept(op.get());
					}
				} finally {
					consumersLock.unlock();
				}
			}
		});
		this.disruptor.handleExceptionsWith(new ExceptionHandler() {
			@Override public void handleEventException(Throwable ex, long sequence, Object event) {
				for(Registration<? extends Consumer<? extends Throwable>> reg : errorConsumers.select(ex.getClass())) {
					((Consumer<Throwable>)reg.getObject()).accept(ex);
				}
			}

			@Override public void handleOnStartException(Throwable ex) {
				handleEventException(ex, -1, null);
			}

			@Override public void handleOnShutdownException(Throwable ex) {
				handleEventException(ex, -1, null);
			}
		});

		this.ringBuffer = this.disruptor.start();
	}

	public void shutdown() {
		disruptor.shutdown();
	}

	public <E extends Throwable> Processor<T> when(Class<E> type, Consumer<E> errorConsumer) {
		consumersLock.lock();
		try {
			errorConsumers.register(Selectors.type(type), errorConsumer);
		} finally {
			consumersLock.unlock();
		}
		return this;
	}

	public Processor<T> consume(Consumer<T> consumer) {
		consumersLock.lock();
		try {
			this.consumers.add(consumer);
		} finally {
			consumersLock.unlock();
		}
		return this;
	}

	public Processor<T> consume(Collection<Consumer<T>> consumers) {
		consumersLock.lock();
		try {
			this.consumers.addAll(consumers);
		} finally {
			consumersLock.unlock();
		}
		return this;
	}

	public Processor<T> cancel(Consumer<T> consumer) {
		consumersLock.lock();
		try {
			this.consumers.remove(consumer);
		} finally {
			consumersLock.unlock();
		}
		return this;
	}

	public Processor<T> cancel(Collection<Consumer<T>> consumers) {
		consumersLock.lock();
		try {
			this.consumers.removeAll(consumers);
		} finally {
			consumersLock.unlock();
		}
		return this;
	}

	public Operation<T> prepare() {
		long seqId = ringBuffer.next();
		Operation<T> op = ringBuffer.get(seqId);
		op.setId(seqId);
		return op;
	}

	public Processor<T> batch(int size, Consumer<T> consumer) {
		Assert.isTrue(size > 2 && size < opsBufferSize,
		              "Batch size must be greater than 2 but less than buffer size (" + opsBufferSize + ")");

		long start = -1;
		long end = -1;
		for(int i = 0; i < size; i++) {
			long seqId = ringBuffer.next();
			if(i == 0) {
				start = seqId;
			} else {
				end = seqId;
			}
			consumer.accept(ringBuffer.get(seqId).get());
		}
		ringBuffer.publish(start, end);

		return this;
	}

}
