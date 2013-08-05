package reactor.core.processor.spec;

import java.util.concurrent.Executor;

import reactor.core.processor.Processor;
import reactor.function.Supplier;

/**
 * Specification class to create {@link Processor Processors}.
 *
 * @author Jon Brisbin
 */
public class ProcessorSpec<T> implements Supplier<Processor<T>> {

	private boolean  multiThreadedProducer = false;
	private int      dataBufferSize        = -1;
	private Executor executor              = null;
	private Supplier<T> dataSupplier;

	/**
	 * Use the given {@link Executor}. If not specified, a default single-threaded {@code Executor} is created.
	 *
	 * @param executor
	 * 		the {@link Executor} to use
	 *
	 * @return {@literal this}
	 */
	public ProcessorSpec<T> executor(Executor executor) {
		this.executor = executor;
		return this;
	}

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

	@Override public Processor<T> get() {
		return new Processor<T>(executor,
		                        dataSupplier,
		                        multiThreadedProducer,
		                        dataBufferSize);
	}

}
