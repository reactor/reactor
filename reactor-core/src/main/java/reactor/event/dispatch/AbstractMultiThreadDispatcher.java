package reactor.event.dispatch;

import reactor.alloc.factory.BatchFactorySupplier;
import reactor.function.Supplier;
import reactor.queue.BlockingQueueFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Base implementation for multi-threaded dispatchers
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1
 */
public abstract class AbstractMultiThreadDispatcher extends AbstractLifecycleDispatcher {

	private static final AtomicLongFieldUpdater<AbstractMultiThreadDispatcher> MPSC_ROUTER
			= AtomicLongFieldUpdater.newUpdater(AbstractMultiThreadDispatcher.class, "recursiveTaskRouter");

	private volatile long recursiveTaskRouter = 0L;

	private final int                                   backlog;
	private final int                                   numberThreads;
	private final BlockingQueue<Task>                   recursiveTasks;
	private final BatchFactorySupplier<MultiThreadTask> taskFactory;

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
		this.recursiveTasks = BlockingQueueFactory.createQueue();
	}

	public int getBacklog() {
		return backlog;
	}

	@Override
	protected Task allocateRecursiveTask() {
		return taskFactory.get();
	}

	@Override
	protected void addToTailRecursionPile(Task task) {
		recursiveTasks.add(task);
	}

	protected Task allocateTask() {
		return taskFactory.get();
	}

	protected class MultiThreadTask extends Task {
		@Override
		public void run() {
			route(this);
			if (MPSC_ROUTER.compareAndSet(AbstractMultiThreadDispatcher.this, 0L, Thread.currentThread().getId())) {
				Task t;
				while (null != (t = recursiveTasks.poll())) {
					route(t);
				}
				recursiveTaskRouter = 0L;
			}
		}
	}

}
