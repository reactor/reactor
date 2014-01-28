package reactor.core.alloc.spec;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import reactor.core.alloc.Recyclable;
import reactor.core.alloc.Reference;
import reactor.core.alloc.RingBufferAllocator;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.util.Assert;

import java.util.concurrent.ExecutorService;

/**
 * Helper class for creating a {@link reactor.core.alloc.RingBufferAllocator}. Provides an easy way to specify the
 * {@code EventFactory}, {@code EventHandler}, and {@code ExceptionHandler} via Reactor abstractions of {@link
 * reactor.function.Supplier} and {@link Consumer}.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public class RingBufferAllocatorSpec<T extends Recyclable> implements Supplier<RingBufferAllocator<T>> {

	private String name     = "ring-buffer-allocator";
	private int    ringSize = 1024;

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
				new EventHandler<Reference<T>>() {
					@Override
					public void onEvent(Reference<T> event, long sequence, boolean endOfBatch) throws Exception {
						if(null != eventHandler) {
							eventHandler.accept(event);
						}
					}
				},
				new ExceptionHandler() {
					@Override
					public void handleEventException(Throwable ex, long sequence, Object event) {
						if(null != errorHandler) {
							errorHandler.accept(ex);
						}
					}

					@Override
					public void handleOnStartException(Throwable ex) {
						if(null != errorHandler) {
							errorHandler.accept(ex);
						}
					}

					@Override
					public void handleOnShutdownException(Throwable ex) {
						if(null != errorHandler) {
							errorHandler.accept(ex);
						}
					}
				},
				producerType,
				waitStrategy,
				executor
		);
		// auto-start
		alloc.start();

		return alloc;
	}

}
