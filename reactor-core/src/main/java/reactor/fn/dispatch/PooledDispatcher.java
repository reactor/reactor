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

import reactor.fn.ConsumerInvoker;
import reactor.fn.ConverterAwareConsumerInvoker;
import reactor.fn.Lifecycle;
import reactor.fn.Linkable;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of a {@link Dispatcher} that pools the given delegate {@link Dispatcher}s into a round-robin pool.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class PooledDispatcher implements Dispatcher {

	private final AtomicLong nextDispatcher = new AtomicLong();
	private final int          poolSize;
	private final Dispatcher[] dispatchers;
	private volatile ConsumerInvoker invoker = new ConverterAwareConsumerInvoker();

	@SuppressWarnings({"unchecked"})
	public PooledDispatcher(Dispatcher... dispatchers) {
		this.poolSize = dispatchers.length;
		this.dispatchers = dispatchers;
		this.start();
	}

	@Override
	public ConsumerInvoker getConsumerInvoker() {
		return invoker;
	}

	@Override
	public PooledDispatcher setConsumerInvoker(ConsumerInvoker consumerInvoker) {
		this.invoker = consumerInvoker;
		return this;
	}

	@Override
	public <T> Task<T> nextTask() {
		return dispatchers[(int) (nextDispatcher.incrementAndGet() % poolSize)].nextTask();
	}

	@Override
	public Lifecycle destroy() {
		for (Dispatcher dispatcher : dispatchers) {
			dispatcher.destroy();
		}
		return this;
	}

	@Override
	public Lifecycle stop() {
		for (Dispatcher dispatcher : dispatchers) {
			dispatcher.stop();
		}
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Lifecycle start() {
		for (Dispatcher d : dispatchers) {
			d.setConsumerInvoker(invoker);
			for (int i = 0; i < poolSize; i++) {
				if (d != dispatchers[i] && d instanceof Linkable) {
					((Linkable<Dispatcher>) d).link(dispatchers[i]);
				}
			}
		}
		return this;
	}

	@Override
	public boolean isAlive() {
		return true;
	}

}
