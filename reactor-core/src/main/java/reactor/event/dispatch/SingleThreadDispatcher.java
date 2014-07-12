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

	private int tailRecurseSeq        = -1;
	private int tailRecursionPileSize = 0;

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
		int toAdd = amount * 2;
		for (int i = 0; i < toAdd; i++) {
			tailRecursionPile.add(new SingleThreadTask());
		}
		this.tailRecursionPileSize += toAdd;
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
			int next = -1;
			while (next < tailRecurseSeq) {
				route(tailRecursionPile.get(++next));
			}

			// clean up extra tasks
			next = tailRecurseSeq;
			int max = backlog * 2;
			while (next >= max) {
				tailRecursionPile.remove(next--);
			}
			tailRecursionPileSize = max;
			tailRecurseSeq = -1;
		}
	}


}
