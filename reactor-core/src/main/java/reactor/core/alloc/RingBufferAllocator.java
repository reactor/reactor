package reactor.core.alloc;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import reactor.event.lifecycle.Lifecycle;
import reactor.function.Supplier;
import reactor.support.Identifiable;
import reactor.support.NamedDaemonThreadFactory;
import reactor.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * An {@code Allocator} implementation based int the LMAX Disruptor {@code RingBuffer}.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public class RingBufferAllocator<T extends Recyclable> implements Allocator<T>, Lifecycle {

	private final ExecutorService executor;
	private final boolean         shutdownExecutor;

	private final Disruptor<Reference<T>>  disruptor;
	private       RingBuffer<Reference<T>> ringBuffer;

	public RingBufferAllocator(String name, int poolSize, Supplier<T> poolFactory) {
		this(name, poolSize, poolFactory, 1);
	}

	public RingBufferAllocator(String name, int poolSize, Supplier<T> poolFactory, int eventThreads) {
		this(name, poolSize, poolFactory, eventThreads, null, null, ProducerType.MULTI, new BlockingWaitStrategy(), null);
	}

	@SuppressWarnings("unchecked")
	public RingBufferAllocator(String name,
	                           int poolSize,
	                           final Supplier<T> poolFactory,
	                           int eventThreads,
	                           final EventHandler<Reference<T>> eventHandler,
	                           final ExceptionHandler errorHandler,
	                           ProducerType producerType,
	                           WaitStrategy waitStrategy,
	                           ExecutorService executor) {
		if(null == executor) {
			this.executor = Executors.newFixedThreadPool(eventThreads, new NamedDaemonThreadFactory(name));
			this.shutdownExecutor = true;
		} else {
			this.executor = executor;
			this.shutdownExecutor = false;
		}

		this.disruptor = new Disruptor<Reference<T>>(
				new EventFactory<Reference<T>>() {
					@SuppressWarnings("rawtypes")
					@Override
					public Reference<T> newInstance() {
						return new RingBufferReference(poolFactory.get());
					}
				},
				poolSize,
				this.executor,
				producerType,
				waitStrategy
		);
		if(null != errorHandler) {
			disruptor.handleExceptionsWith(errorHandler);
		}
		if(null != eventHandler) {
			if(eventThreads > 1) {
				WorkHandler<Reference<T>>[] workHandlers = new WorkHandler[eventThreads];
				for(int i = 0; i < eventThreads; i++) {
					workHandlers[i] = new WorkHandler<Reference<T>>() {
						@Override
						public void onEvent(Reference<T> ref) throws Exception {
							eventHandler.onEvent(ref, -1, false);
						}
					};
				}
				disruptor.handleEventsWithWorkerPool(workHandlers);
			} else {
				disruptor.handleEventsWith(eventHandler);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Reference<T> allocate() {
		long l = ringBuffer.next();
		RingBufferReference ref = (RingBufferReference)ringBuffer.get(l);
		ref.setSequenceId(l);
		ref.retain();
		return ref;
	}

	@Override
	public List<Reference<T>> allocateBatch(int size) {
		long max = ringBuffer.remainingCapacity();
		Assert.isTrue(size < ringBuffer.remainingCapacity(),
		              "Cannot allocate more than " + max + " at a time with this RingBuffer configuration.");
		List<Reference<T>> batch = new ArrayList<Reference<T>>(size);
		for(int i = 0; i < size; i++) {
			batch.add(allocate());
		}
		return batch;
	}

	@Override
	public void release(List<Reference<T>> batch) {
		if(null == batch || batch.isEmpty()) {
			return;
		}
		long start = ((RingBufferReference)batch.get(0)).sequenceId;
		int len = batch.size();
		if(len > 1) {
			long end = ((RingBufferReference)batch.get(len - 1)).sequenceId;
			ringBuffer.publish(start, end);
		} else {
			if(!ringBuffer.isPublished(start)) {
				ringBuffer.publish(start);
			}
		}
		for(Reference<T> ref : batch) {
			ref.release();
		}
	}

	@Override
	public boolean alive() {
		return !executor.isShutdown();
	}

	@Override
	public void start() {
		ringBuffer = disruptor.start();
	}

	@Override
	public boolean awaitAndShutdown() {
		return awaitAndShutdown(Integer.MAX_VALUE, TimeUnit.SECONDS);
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		try {
			if(shutdownExecutor) {
				return executor.awaitTermination(timeout, timeUnit);
			}
		} catch(InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		} finally {
			shutdown();
		}
		return true;
	}

	@Override
	public void shutdown() {
		disruptor.shutdown();
		if(shutdownExecutor) {
			executor.shutdown();
		}
	}

	@Override
	public void halt() {
		if(shutdownExecutor) {
			executor.shutdownNow();
		}
		disruptor.halt();
	}

	private class RingBufferReference extends AbstractReference<T> {
		private final    boolean isIdentifiable;
		private volatile long    sequenceId;

		private RingBufferReference(T obj) {
			super(obj);
			this.isIdentifiable = Identifiable.class.isInstance(obj);
		}

		@SuppressWarnings("unchecked")
		public void setSequenceId(long sequenceId) {
			this.sequenceId = sequenceId;
			if(isIdentifiable) {
				((Identifiable<Long>)get()).setId(sequenceId);
			}
		}

		@Override
		public void release(int decr) {
			if(!ringBuffer.isPublished(sequenceId)) {
				// No one else is currently accessing this reference so we can
				// publish to the RingBuffer, causing the EventHandler to run.
				ringBuffer.publish(sequenceId);
				// Don't actually release this until the EventHandler has run.
				// This is a different situation than other Reference implementations
				// that usually want immediately clearing of resources.
				if(1 == decr) {
					return;
				}
			}
			super.release(decr);
		}
	}

}
