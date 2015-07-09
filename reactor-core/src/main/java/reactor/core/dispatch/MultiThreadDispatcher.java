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

import reactor.core.processor.InsufficientCapacityException;

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

	protected MultiThreadDispatcher(int numberThreads, int backlog) {
		this.backlog = backlog;
		this.numberThreads = numberThreads;
	}

	@Override
	public boolean supportsOrdering() {
		return false;
	}

	@Override
	public long backlogSize() {
		return backlog;
	}

	public int poolSize() {
		return numberThreads;
	}

	@Override
	protected void scheduleLater(Task task) {
		execute(task);
	}

	@Override
	protected Task allocateRecursiveTask() {
		return allocateTask();
	}

	protected Task allocateTask() {
		return new MultiThreadTask();
	}

	@Override
	protected Task tryAllocateTask() throws InsufficientCapacityException {
		return allocateTask();
	}

	protected class MultiThreadTask extends Task {
		@Override
		public void run() {
			route(this);
		}
	}

}
