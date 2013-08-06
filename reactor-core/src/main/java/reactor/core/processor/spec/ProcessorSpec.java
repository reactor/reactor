package reactor.core.processor.spec;

import reactor.core.processor.Processor;
import reactor.event.registry.CachingRegistry;
import reactor.event.registry.Registry;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Supplier;

/**
 * Specification class to create {@link Processor Processors}.
 *
 * @author Jon Brisbin
 */
public class ProcessorSpec<T> implements Supplier<Processor<T>> {

	private Registry<Consumer<Throwable>> errorConsumers        = new CachingRegistry<Consumer<Throwable>>();
	private boolean                       multiThreadedProducer = false;
	private int                           dataBufferSize        = -1;
	private Consumer<T> consumer;
	private Supplier<T> dataSupplier;

	/**
	 * Protect against publication of data events from multiple producer threads.
	 *
	 * @return {@literal this}
	 */
	public ProcessorSpec<T> multiThreadedProducer() {
		this.multiThreadedProducer = true;
		return this;
	}

	/**
	 * Optimize for highest throughput by assuming only a single thread will be publishing data events into this {@code
	 * Processor}.
	 *
	 * @return {@literal this}
	 */
	public ProcessorSpec<T> singleThreadedProducer() {
		this.multiThreadedProducer = false;
		return this;
	}

	/**
	 * How many data objects to pre-allocate in the buffer.
	 *
	 * @param dataBufferSize
	 * 		number of data objects to pre-allocate
	 *
	 * @return {@literal this}
	 */
	public ProcessorSpec<T> dataBufferSize(int dataBufferSize) {
		this.dataBufferSize = dataBufferSize;
		return this;
	}

	/**
	 * Use the given {@link Supplier} to provide new instances of the data object for pre-allocation.
	 *
	 * @param dataSupplier
	 * 		the {@link Supplier} to provide new data instances
	 *
	 * @return {@literal this}
	 */
	public ProcessorSpec<T> dataSupplier(Supplier<T> dataSupplier) {
		this.dataSupplier = dataSupplier;
		return this;
	}

	/**
	 * When data is mutated and published into the {@code Processor}, invoke the given {@link Consumer} and pass the
	 * mutated data.
	 *
	 * @param consumer
	 * 		the mutated event data {@code Consumer}
	 *
	 * @return {@literal this}
	 */
	public ProcessorSpec<T> consume(Consumer<T> consumer) {
		this.consumer = consumer;
		return this;
	}

	/**
	 * Assign the given {@link Consumer} as an error handler for exceptions of the given type.
	 *
	 * @param type
	 * 		type of the exception to handle
	 * @param errorConsumer
	 * 		exception {@code Consumer}
	 *
	 * @return {@literal this}
	 */
	public ProcessorSpec<T> when(Class<? extends Throwable> type, Consumer<Throwable> errorConsumer) {
		errorConsumers.register(Selectors.type(type), errorConsumer);
		return this;
	}


	@Override public Processor<T> get() {
		return new Processor<T>(dataSupplier,
		                        consumer,
		                        errorConsumers,
		                        multiThreadedProducer,
		                        dataBufferSize);
	}

}
