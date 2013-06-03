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

/**
 * @author Jon Brisbin
 */
public abstract class AbstractDispatcher implements Dispatcher {

	private final AtomicBoolean   alive   = new AtomicBoolean(true);
	private final ConsumerInvoker invoker = new ArgumentConvertingConsumerInvoker(null);

	@Override
	public boolean alive() {
		return alive.get();
	}

	@Override
	public void shutdown() {
		alive.compareAndSet(true, false);
	}

	@Override
	public void halt() {
		alive.compareAndSet(true, false);
	}

	@Override
	public <T> Task<T> nextTask() {
		if (!alive()) {
			throw new IllegalStateException("This Dispatcher has been shutdown and cannot accept new tasks.");
		}
		return createTask();
	}

	protected ConsumerInvoker getInvoker() {
		return invoker;
	}

	protected abstract <T> Task<T> createTask();

}
