/*
 * Copyright (c) 2011-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.fn.dispatch;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Jon Brisbin
 */
public abstract class AbstractDispatcher implements Dispatcher {

	private final AtomicInteger   queuedTasks = new AtomicInteger();
	private final AtomicBoolean   alive       = new AtomicBoolean(true);
	private final ConsumerInvoker invoker     = new ConverterAwareConsumerInvoker();

	@Override
	public boolean alive() {
		return alive.get();
	}

	@Override
	public boolean shutdown() {
		alive.compareAndSet(true, false);
		return queuedTasks.get() > 0;
	}

	@Override
	public boolean halt() {
		alive.compareAndSet(true, false);
		return queuedTasks.get() > 0;
	}

	@Override
	public <T> Task<T> nextTask() {
		if (!alive()) {
			throw new IllegalStateException("This Dispatcher has been shutdown and cannot accept new tasks.");
		}
		incrementTaskCount();
		return createTask();
	}

	protected ConsumerInvoker getInvoker() {
		return invoker;
	}

	protected void incrementTaskCount() {
		queuedTasks.incrementAndGet();
	}

	protected void decrementTaskCount() {
		queuedTasks.decrementAndGet();
	}

	protected abstract <T> Task<T> createTask();

}
