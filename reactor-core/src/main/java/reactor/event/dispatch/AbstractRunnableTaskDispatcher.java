package reactor.event.dispatch;

import reactor.core.alloc.Allocator;
import reactor.core.alloc.Reference;
import reactor.core.alloc.ReferenceCountingAllocator;
import reactor.event.Event;
import reactor.event.registry.Registry;
import reactor.event.routing.EventRouter;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.queue.BlockingQueueFactory;

import java.util.concurrent.BlockingQueue;

/**
 * @author Jon Brisbin
 */
public abstract class AbstractRunnableTaskDispatcher extends AbstractLifecycleDispatcher {

	private final BlockingQueue<RunnableTask> tailRecursionPile;
	private       Allocator<RunnableTask>     taskAllocator;
	private       Allocator<RunnableTask>     inContextTaskAllocator;

	protected AbstractRunnableTaskDispatcher() {
		this(1024, null);
	}

	protected AbstractRunnableTaskDispatcher(int backlogSize, Allocator<RunnableTask> taskAllocator) {
		if(null != taskAllocator) {
			this.taskAllocator = taskAllocator;
		} else {
			this.taskAllocator = new ReferenceCountingAllocator<RunnableTask>(
					backlogSize,
					new Supplier<RunnableTask>() {
						@Override
						public RunnableTask get() {
							return new RunnableTask();
						}
					}
			);
		}

		this.inContextTaskAllocator = new ReferenceCountingAllocator<RunnableTask>(
				backlogSize,
				new Supplier<RunnableTask>() {
					@Override
					public RunnableTask get() {
						return new RunnableTask();
					}
				}
		);

		this.tailRecursionPile = BlockingQueueFactory.createQueue();
	}

	protected void setTaskAllocator(Allocator<RunnableTask> taskAllocator) {
		this.taskAllocator = taskAllocator;
	}

	protected BlockingQueue<RunnableTask> getTailRecursionPile() {
		return tailRecursionPile;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <E extends Event<?>> void dispatch(final Object key,
	                                          final E event,
	                                          final Registry<Consumer<? extends Event<?>>> consumerRegistry,
	                                          final Consumer<Throwable> errorConsumer,
	                                          final EventRouter eventRouter,
	                                          final Consumer<E> completionConsumer) {
		if(!alive()) {
			throw new IllegalStateException("This Dispatcher has been shutdown");
		}

		boolean isInContext = isInContext();
		RunnableTask task = allocateTask(isInContext);

		task.setKey(key)
		    .setEvent(event)
		    .setConsumerRegistry(consumerRegistry)
		    .setErrorConsumer(errorConsumer)
		    .setEventRouter(eventRouter)
		    .setCompletionConsumer(completionConsumer);

		if(isInContext) {
			tailRecursionPile.offer(task);
		} else {
			submit(task);
		}
	}

	protected Allocator<RunnableTask> getTaskAllocator(boolean inContext) {
		return (inContext ? inContextTaskAllocator : taskAllocator);
	}

	protected RunnableTask allocateTask(boolean inContext) {
		Reference<RunnableTask> ref = getTaskAllocator(inContext).allocate();
		ref.get().setReference(ref);
		return ref.get();
	}

	protected abstract void submit(RunnableTask task);

	protected class RunnableTask extends Task {
		private Reference<RunnableTask> reference;

		public Reference<RunnableTask> getReference() {
			return reference;
		}

		public RunnableTask setReference(Reference<RunnableTask> reference) {
			this.reference = reference;
			return this;
		}

		@Override
		public void run() {
			try {
				eventRouter.route(key,
				                  event,
				                  (null != consumerRegistry ? consumerRegistry.select(key) : null),
				                  completionConsumer,
				                  errorConsumer);
			} catch(Throwable t) {
				if(null != errorConsumer) {
					errorConsumer.accept(t);
				}
			}

			//Process any lazy notify
			RunnableTask recursiveTask;
			while(null != (recursiveTask = tailRecursionPile.poll())) {
				recursiveTask.eventRouter.route(
						recursiveTask.key,
						recursiveTask.event,
						(null != recursiveTask.consumerRegistry
						 ? recursiveTask.consumerRegistry.select(recursiveTask.key)
						 : null),
						recursiveTask.completionConsumer,
						recursiveTask.errorConsumer
				);

				if(null != recursiveTask.getReference() && recursiveTask.getReference().getReferenceCount() > 0) {
					recursiveTask.getReference().release();
				}
			}

			if(null != getReference() && getReference().getReferenceCount() > 0) {
				getReference().release();
			}
		}
	}

}
