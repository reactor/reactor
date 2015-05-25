/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.processor;

import reactor.core.processor.util.SingleUseExecutor;
import reactor.core.support.Exceptions;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A base processor used by executor backed processors to take care of their ExecutorService
 *
 * @author Stephane Maldini
 */
public abstract class ExecutorPoweredProcessor<IN, OUT> extends ReactorProcessor<IN, OUT> {

	protected final ExecutorService executor;

	protected ExecutorPoweredProcessor(String name, ExecutorService executor, boolean autoCancel) {
		super(autoCancel);

		this.executor = executor == null
				? SingleUseExecutor.create(name)
				: executor;
	}


	@Override
	public void onComplete() {
		if (executor.getClass() == SingleUseExecutor.class) {
			executor.shutdown();
		}
	}

	@Override
	public boolean awaitAndShutdown() {
		return awaitAndShutdown(-1, TimeUnit.SECONDS);
	}

	@Override
	public boolean awaitAndShutdown(long timeout, TimeUnit timeUnit) {
		try {
			shutdown();
			return executor.awaitTermination(timeout, timeUnit);
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			return false;
		}
	}

	@Override
	public void forceShutdown() {
		if (executor.isShutdown()) return;
		executor.shutdownNow();
	}

	@Override
	public boolean alive() {
		return !executor.isTerminated();
	}

	@Override
	public void shutdown() {
		try {
			onComplete();
			executor.shutdown();
		} catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			onError(t);
		}
	}

}
