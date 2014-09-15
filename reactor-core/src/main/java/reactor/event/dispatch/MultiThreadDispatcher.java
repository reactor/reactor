package reactor.event.dispatch;

import reactor.alloc.factory.BatchFactorySupplier;
import reactor.function.Supplier;

/**
 * Base implementation for multi-threaded dispatchers
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1
 */
public abstract class MultiThreadDispatcher extends AbstractLifecycleDispatcher {

	private final int                                   backlog;
	private final int                                   numberThreads;
	private final BatchFactorySupplier<MultiThreadTask> taskFactory;

	protected MultiThreadDispatcher(int numberThreads, int backlog) {
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
	}

	@Override
	public boolean supportsOrdering() {
		return false;
	}

	@Override
	public int backlogSize() {
		return backlog;
	}

	public int poolSize() {
		return numberThreads;
	}

	@Override
	protected Task allocateRecursiveTask() {
		throw new IllegalStateException("MultiThreadDispatchers cannot allocate recursively");
	}

	protected Task allocateTask() {
		return taskFactory.get();
	}

	@Override
	public boolean inContext() {
		return false;
	}

	protected class MultiThreadTask extends Task {
		@Override
		public void run() {
			route(this);
		}
	}

}
