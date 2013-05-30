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

/**
 * A {@link Dispatcher} implementation that executes a {@link Task} immediately in the calling thread.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class SynchronousDispatcher implements Dispatcher {

	public static final Dispatcher INSTANCE = new SynchronousDispatcher();

	private final ConsumerInvoker invoker = new ConverterAwareConsumerInvoker();

	@Override
	@SuppressWarnings({"unchecked"})
	public <T> Task<T> nextTask() {
		return (Task<T>) new SyncTask();
	}

	@Override
	public boolean alive() {
		return true;
	}

	@Override
	public boolean shutdown() {
		return false;
	}

	@Override
	public boolean halt() {
		return false;
	}

	private class SyncTask extends Task<Object> {
		@Override
		public void submit() {
			execute(invoker);
		}
	}

}
