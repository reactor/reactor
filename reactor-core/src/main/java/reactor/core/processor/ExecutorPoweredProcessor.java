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

import org.reactivestreams.Subscription;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.support.SignalType;
import reactor.core.support.SingleUseExecutor;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.fn.timer.GlobalTimer;
import reactor.fn.timer.Timer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;

/**
 * A base processor used by executor backed processors to take care of their ExecutorService
 *
 * @author Stephane Maldini
 */
public abstract class ExecutorPoweredProcessor<IN, OUT> extends BaseProcessor<IN, OUT> {

	protected final ExecutorService executor;

	private volatile boolean terminated;

	protected ExecutorPoweredProcessor(String name, ExecutorService executor, boolean autoCancel) {
		super(
		  executor == null ?
			new ClassLoader(Thread.currentThread().getContextClassLoader()) {
			} :
			null,
		  autoCancel);
		if (executor == null) {
			this.executor = SingleUseExecutor.create(name, contextClassLoader);
		} else {
			this.executor = executor;
		}
	}

	@Override
	public void onSubscribe(Subscription s) {
		Subscription subscription = upstreamSubscription;
		super.onSubscribe(s);
		if (subscription == null && s != SignalType.NOOP_SUBSCRIPTION) {
			requestTask(s);
		}
	}

	protected void requestTask(final Subscription s) {
		//implementation might run a specific request task for the given subscription
	}

	@Override
	public void onComplete() {
		upstreamSubscription = null;
		terminated = true;
	}

	@Override
	public void onError(Throwable t) {
		super.onError(t);
		upstreamSubscription = null;
		terminated = true;
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
	protected int decrementSubscribers() {
		int subs = super.decrementSubscribers();
		if (autoCancel && subs == 0 && executor.getClass() == SingleUseExecutor.class) {
			if (BaseProcessor.CANCEL_TIMEOUT > 0) {
				final Timer timer = GlobalTimer.globalOrNew();
				timer.submit(new Consumer<Long>() {
					@Override
					public void accept(Long aLong) {
						if (SUBSCRIBER_COUNT.get(ExecutorPoweredProcessor.this) == 0) {
							terminated = true;
							executor.shutdown();
						}
						timer.cancel();
					}
				}, BaseProcessor.CANCEL_TIMEOUT, TimeUnit.SECONDS);
			} else {
				terminated = true;
				executor.shutdown();
			}
		}
		return subs;
	}

	@Override
	public void forceShutdown() {
		if (executor.isShutdown()) return;
		executor.shutdownNow();
	}

	@Override
	public boolean alive() {
		return !terminated;
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

	/**
	 * @return true if the attached Subscribers will read exclusive sequences (akin to work-queue pattern)
	 */
	public abstract boolean isWork();

}
