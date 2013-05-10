/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package reactor.fn.dispatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.fn.*;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class SynchronousDispatcher implements Dispatcher {

	private static final Logger          LOG     = LoggerFactory.getLogger(SynchronousDispatcher.class);
	private volatile     ConsumerInvoker invoker = new ConverterAwareConsumerInvoker();

	@Override
	public ConsumerInvoker getConsumerInvoker() {
		return invoker;
	}

	@Override
	public SynchronousDispatcher setConsumerInvoker(ConsumerInvoker consumerInvoker) {
		this.invoker = consumerInvoker;
		return this;
	}

	@Override
	@SuppressWarnings({"unchecked"})
	public <T> Task<T> nextTask() {
		return (Task<T>) new SyncTask();
	}

	@Override
	public Lifecycle destroy() {
		return this;
	}

	@Override
	public Lifecycle stop() {
		return this;
	}

	@Override
	public Lifecycle start() {
		return this;
	}

	@Override
	public boolean isAlive() {
		return true;
	}

	private class SyncTask extends Task<Object> {
		@Override
		public void submit() {
			try {
				for (Registration<? extends Consumer<? extends Event<?>>> reg : getConsumerRegistry().select(getSelector())) {
					if (reg.isCancelled() || reg.isPaused()) {
						continue;
					}
					reg.getSelector().setHeaders(getSelector(), getEvent());
					invoker.invoke(reg.getObject(), getConverter(), Void.TYPE, getEvent());
					if (reg.isCancelAfterUse()) {
						reg.cancel();
					}
				}
				if (null != getCompletionConsumer()) {
					invoker.invoke(getCompletionConsumer(), getConverter(), Void.TYPE, getEvent());
				}
			} catch (Throwable x) {
				LOG.error(x.getMessage(), x);
				if (null != getErrorConsumer()) {
					getErrorConsumer().accept(x);
				}
			}
		}
	}

}
