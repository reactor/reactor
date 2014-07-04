package reactor.event.dispatch;

import java.util.ArrayList;
import java.util.List;

/**
 * Base Implementation for single-threaded Dispatchers.
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1
 */
public abstract class SingleThreadDispatcher extends AbstractLifecycleDispatcher {

	private final List<Task> tailRecursionPile = new ArrayList<Task>();
	private final int backlog;

	private volatile int tailRecurseSeq        = -1;
	private volatile int tailRecursionPileSize = 0;

	public SingleThreadDispatcher(int backlog) {
		this.backlog = backlog;
		expandTailRecursionPile(backlog);
	}


	@Override
	public boolean supportsOrdering() {
		return true;
	}

	@Override
	public int backlogSize() {
		return backlog;
	}

	public int getTailRecursionPileSize() {
		return tailRecursionPileSize;
	}

	protected void expandTailRecursionPile(int amount) {
		for (int i = 0; i < amount; i++) {
			tailRecursionPile.add(new SingleThreadTask());
		}
		this.tailRecursionPileSize = tailRecursionPile.size();
	}

	protected Task allocateRecursiveTask() {
		int next = ++tailRecurseSeq;
		if (next == tailRecursionPileSize) {
			expandTailRecursionPile(backlog);
		}
		return tailRecursionPile.get(next);
	}

	protected abstract Task allocateTask();

	protected class SingleThreadTask extends Task {
		@Override
		public void run() {
			route(this);

			//Process any recursive tasks
			if (tailRecurseSeq < 0) {
				return;
			}
			Task task;
			int next = 0;
			while (next <= tailRecurseSeq) {
				task = tailRecursionPile.get(next++);
				route(task);
			}
			// clean up extra tasks
			next = tailRecurseSeq;
			while (next > backlog) {
				tailRecursionPile.remove(next--);
			}
			tailRecurseSeq = -1;
		}
	}


}
