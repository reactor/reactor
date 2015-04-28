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
import reactor.core.Dispatcher;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Pausable;
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


	private final Consumer<Long> timeoutTask;

	private final Consumer<Void> timeoutRequest = new Consumer<Void>() {
		@Override
		public void accept(Void aVoid) {
			if (fallback != null) {
				doSwitch();
			} else {
				doError(new TimeoutException("No data signaled for " + timeout + "ms"));
			}
		}
	};

	private Pausable timeoutRegistration;

	public TimeoutAction(final Dispatcher dispatcher, Publisher<? extends T> fallback, Timer timer, long timeout) {
		super(fallback);
		Assert.state(timer != null, "Timer must be supplied");
		this.timeoutTask = new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				dispatcher.dispatch(null, timeoutRequest, null);
			}
		};
		this.timer = timer;
		this.timeout = timeout;
	}

	@Override
	public void requestMore(long req) {
		synchronized (this) {
			if (timeoutRegistration != null) timeoutRegistration.cancel();
			timeoutRegistration = timer.submit(timeoutTask, timeout, TimeUnit.MILLISECONDS);
		}
		super.requestMore(req);
	}


	@Override
	protected void doNormalNext(T ev) {
		synchronized (this) {
			if (timeoutRegistration != null) timeoutRegistration.cancel();
			timeoutRegistration = timer.submit(timeoutTask, timeout, TimeUnit.MILLISECONDS);
		}
		broadcastNext(ev);
	}

	@Override
	protected void doShutdown() {
		if (timeoutRegistration != null) {
			timeoutRegistration.cancel();
			timeoutRegistration = null;
		}
		super.doShutdown();
	}
}
