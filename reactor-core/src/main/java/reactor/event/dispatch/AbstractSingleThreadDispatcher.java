package reactor.event.dispatch;

import java.util.ArrayList;
import java.util.List;

/**
 * Base Implementation for Single Threaded Dispatchers
 *
 * @author Stephane Maldini
 * @author Jon Brisbin
 *
 * @since 1.1
 */
public abstract class AbstractSingleThreadDispatcher extends AbstractLifecycleDispatcher {

	private final List<Task> tailRecursionPile = new ArrayList<Task>();
	private final int backlog;

	private int tailRecurseSeq        = -1;
	private int tailRecursionPileSize = 0;

	public AbstractSingleThreadDispatcher(int backlog) {
		this.backlog = backlog;
		expandTailRecursionPile(backlog);
	}

	public int getBacklog() {
		return backlog;
	}

	protected void expandTailRecursionPile(int amount) {
		for(int i = 0; i < amount; i++) {
			tailRecursionPile.add(new SingleThreadTask());
		}
		this.tailRecursionPileSize = tailRecursionPile.size();
	}

	protected Task allocateRecursiveTask() {
		int next = ++tailRecurseSeq;
		if(next == tailRecursionPileSize) {
			expandTailRecursionPile(backlog);
		}
		return tailRecursionPile.get(next);
	}

	protected abstract Task allocateTask();

	protected abstract void submit(Task task);

	protected class SingleThreadTask extends Task {
		@Override
		public void run() {
			route(this);

			//Process any recursive tasks
			if(tailRecurseSeq < 0) {
				return;
			}
			Task task;
			int next = 0;
			while(next <= tailRecurseSeq) {
				task = tailRecursionPile.get(next++);
				route(task);
			}
			// clean up extra tasks
			next = tailRecurseSeq;
			while(next > backlog) {
				tailRecursionPile.remove(next--);
			}
			tailRecurseSeq = -1;
		}
	}


}
