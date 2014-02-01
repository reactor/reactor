package reactor.event.dispatch;

import reactor.queue.BlockingQueueFactory;

import java.util.concurrent.BlockingQueue;

/**
 * @author Jon Brisbin
 */
public abstract class AbstractMultiThreadDispatcher extends AbstractLifecycleDispatcher {

	private final BlockingQueue<Task> tasks;
	private final BlockingQueue<Task> tailRecursionPile;
	private final int                 backlog;

	protected AbstractMultiThreadDispatcher(int backlog) {
		this.backlog = backlog;
		this.tasks = BlockingQueueFactory.createQueue();
		this.tailRecursionPile = BlockingQueueFactory.createQueue();

		for(int i = 0; i < backlog; i++) {
			tasks.add(new MultiThreadTask());
		}
	}

	public int getBacklog() {
		return backlog;
	}

	@Override
	protected void addToTailRecursionPile(Task task) {
		tailRecursionPile.add(task);
	}

	protected Task allocateRecursiveTask() {
		return allocateTask();
	}

	protected Task allocateTask() {
		Task task = tasks.poll();
		if(null != task) {
			return task;
		} else {
			return new MultiThreadTask();
		}
	}

	protected class MultiThreadTask extends Task {
		@Override
		public void run() {
			route(this);

			//Process any recursive tasks
			if(!tailRecursionPile.isEmpty()) {
				Task task;
				while(null != (task = tailRecursionPile.poll())) {
					route(task);
					tasks.offer(task);
				}
			}

			tasks.offer(this);
		}
	}

}
