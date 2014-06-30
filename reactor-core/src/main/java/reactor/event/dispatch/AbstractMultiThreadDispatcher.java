package reactor.event.dispatch;

import reactor.alloc.factory.BatchFactorySupplier;
import reactor.function.Supplier;
import reactor.util.PartitionedReferencePile;

/**
 * Base implementation for multi-threaded dispatchers
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1
 */
public abstract class AbstractMultiThreadDispatcher extends AbstractLifecycleDispatcher {

	private final int                                       backlog;
	private final int                                       numberThreads;
	private final PartitionedReferencePile<MultiThreadTask> tailRecursionPile;
	private final BatchFactorySupplier<MultiThreadTask>     taskFactory;

	protected AbstractMultiThreadDispatcher(int numberThreads, int backlog) {
		this.backlog = backlog;
		this.numberThreads = numberThreads;
		this.taskFactory = new BatchFactorySupplier<MultiThreadTask>(
				backlog,
				new Supplier<MultiThreadTask>() {
					@Override
					public MultiThreadTask get() {
						return new MultiThreadTask();
					}
				}
		);
		this.tailRecursionPile = new PartitionedReferencePile<MultiThreadTask>(
				numberThreads,
				taskFactory
		);
	}

	public int getBacklog() {
		return backlog;
	}

	@Override
	protected Task allocateRecursiveTask() {
		return tailRecursionPile.get();
	}

	protected Task allocateTask() {
		return taskFactory.get();
	}

	protected class MultiThreadTask extends Task {
		@Override
		public void run() {
			route(this);

			for (MultiThreadTask t : tailRecursionPile) {
				route(t);
			}
		}
	}

}
