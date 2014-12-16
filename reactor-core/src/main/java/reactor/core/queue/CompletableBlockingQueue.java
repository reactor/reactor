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

package reactor.core.queue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A {@literal LinkedQueue} that supports a terminal state.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
final public class CompletableBlockingQueue<T> extends ArrayBlockingQueue<T> implements CompletableQueue<T>{

	boolean terminated = false;

	public CompletableBlockingQueue(int capacity) {
		super(capacity);
	}

	@Override
	public void complete() {
		terminated = true;
	}

	@Override
	public boolean isComplete() {
		return terminated;
	}

	@Override
	public T take() throws InterruptedException {
		if(terminated && isEmpty()) return null;
		return super.take();
	}

	@Override
	public T poll() {
		if(terminated && isEmpty()) return null;
		return super.poll();
	}

	@Override
	public T poll(long timeout, TimeUnit unit) throws InterruptedException {
		if(terminated && isEmpty()) return null;
		return super.poll(timeout, unit);
	}
}
