/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.core.dispatch;

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
	public long backlogSize() {
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
