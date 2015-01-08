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
package reactor.rx.action.error;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.bus.registry.Registration;
import reactor.core.Dispatcher;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.timer.Timer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public final class TimeoutAction<T> extends FallbackAction<T> {

	private final Timer timer;
	private final long  timeout;


	private final Consumer<Long> timeoutTask = new Consumer<Long>() {
		@Override
		public void accept(Long aLong) {
			if (timeoutRegistration.getObject() == this)
				dispatch(timeoutRequest);
		}
	};

	private final Consumer<Void> timeoutRequest = new Consumer<Void>() {
		@Override
		public void accept(Void aVoid) {
			if (!timeoutRegistration.isCancelled()) {
				if (fallback != null) {
					doSwitch();
				} else {
					doError(new TimeoutException("No data signaled for " + timeout + "ms"));
				}
			}
		}
	};

	private volatile Registration<? extends Consumer<Long>> timeoutRegistration;

	public TimeoutAction(Dispatcher dispatcher, Publisher<? extends T> fallback, Timer timer, long timeout) {
		super(dispatcher, fallback);
		Assert.state(timer != null, "Timer must be supplied");
		this.timer = timer;
		this.timeout = timeout;
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		super.doSubscribe(subscription);
		timeoutRegistration = timer.submit(timeoutTask, timeout, TimeUnit.MILLISECONDS);
	}


	@Override
	protected void doNormalNext(T ev) {
		timeoutRegistration.cancel();
		broadcastNext(ev);
		timeoutRegistration = timer.submit(timeoutTask, timeout, TimeUnit.MILLISECONDS);
	}

	@Override
	public void cancel() {
		if (timeoutRegistration != null) {
			timeoutRegistration.cancel();
			timeoutRegistration = null;
		}
		super.cancel();
	}

	@Override
	public void doComplete() {
		if (timeoutRegistration != null) {
			timeoutRegistration.cancel();
			timeoutRegistration = null;
		}
		super.doComplete();
	}
}
