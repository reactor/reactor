package reactor.event.dispatch;

import reactor.core.alloc.factory.BatchFactorySupplier;
import reactor.event.Event;
import reactor.event.registry.Registry;
import reactor.event.routing.EventRouter;
import reactor.function.Consumer;
import reactor.function.Supplier;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Jon Brisbin
 */
public abstract class AbstractRunnableTaskDispatcher extends AbstractLifecycleDispatcher {

	private final BatchFactorySupplier<RunnableTask> taskSupplier;
	private final List<RunnableTask>                 tailRecursionPile;
	private final int                                backlogSize;
	private int tailRecurseIdx = 0;

	protected AbstractRunnableTaskDispatcher() {
		this(-1, null);
	}

	protected AbstractRunnableTaskDispatcher(int backlogSize, BatchFactorySupplier<RunnableTask> taskSupplier) {
		this.backlogSize = backlogSize;
		if(null != taskSupplier) {
			this.taskSupplier = taskSupplier;
		} else {
			this.taskSupplier = new BatchFactorySupplier<RunnableTask>(
					backlogSize,
					new Supplier<RunnableTask>() {
						@Override
						public RunnableTask get() {
							return new RunnableTask();
						}
					}
			);
		}
		this.tailRecursionPile = new ArrayList<RunnableTask>(backlogSize > 0 ? backlogSize : 1);
		for(int i = 0; i < backlogSize; i++) {
			this.tailRecursionPile.add(new RunnableTask());
		}
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

		RunnableTask task;
		boolean isInContext = isInContext();
		if(isInContext) {
			if(tailRecurseIdx >= backlogSize) {
				task = (null != taskSupplier ? taskSupplier.get() : new RunnableTask());
				tailRecursionPile.add(task);
			} else {
				task = tailRecursionPile.get(tailRecurseIdx);
			}
			tailRecurseIdx++;
		} else {
			task = allocateTask();
		}

		task.setKey(key)
		    .setEvent(event)
		    .setConsumerRegistry(consumerRegistry)
		    .setErrorConsumer(errorConsumer)
		    .setEventRouter(eventRouter)
		    .setCompletionConsumer(completionConsumer);

		if(!isInContext) {
			submit(task);
		}
	}

	protected RunnableTask allocateTask() {
		return taskSupplier.get();
	}

	protected abstract void submit(RunnableTask task);

	protected class RunnableTask extends Task {
		@Override
		public final void run() {
			eventRouter.route(key,
			                  event,
			                  (null != consumerRegistry ? consumerRegistry.select(key) : null),
			                  completionConsumer,
			                  errorConsumer);

			//Process any lazy notify
			Task delayedTask;
			int i = 0;
			while(i < tailRecurseIdx) {
				delayedTask = tailRecursionPile.get(i++);
				delayedTask.eventRouter.route(
						delayedTask.key,
						delayedTask.event,
						(null != delayedTask.consumerRegistry ? delayedTask.consumerRegistry.select(delayedTask.key) : null),
						delayedTask.completionConsumer,
						delayedTask.errorConsumer
				);
				delayedTask.recycle();
			}

			i = tailRecurseIdx - 1;
			while(backlogSize > 0 && i >= backlogSize) {
				tailRecursionPile.remove(i--);
			}

			tailRecurseIdx = 0;
		}
	}


}
