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

import reactor.fn.ConsumerInvoker;
import reactor.fn.support.ConverterAwareConsumerInvoker;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class SynchronousDispatcher implements Dispatcher {

	private final ConsumerInvoker invoker = new ConverterAwareConsumerInvoker();

	@Override
	@SuppressWarnings({"unchecked"})
	public <T> Task<T> nextTask() {
		return (Task<T>) new SyncTask();
	}

	@Override
	public SynchronousDispatcher destroy() {
		return this;
	}

	@Override
	public SynchronousDispatcher stop() {
		return this;
	}

	@Override
	public SynchronousDispatcher start() {
		return this;
	}

	@Override
	public boolean isAlive() {
		return true;
	}

	private class SyncTask extends Task<Object> {
		@Override
		public void submit() {
			execute(invoker);
		}
	}

}
