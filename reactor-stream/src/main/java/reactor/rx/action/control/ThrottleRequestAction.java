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
package reactor.rx.action.control;

import reactor.ReactorProcessor;
import reactor.core.error.InsufficientCapacityException;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Pausable;
import reactor.fn.timer.Timer;
import reactor.rx.action.Action;

import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class ThrottleRequestAction<T> extends Action<T, T> {

	private final Timer          timer;
	private final long           period;
	private final Consumer<Long> periodTask;
	private       long           pending;

	private Pausable timeoutRegistration;

	@SuppressWarnings("unchecked")
	public ThrottleRequestAction(final ReactorProcessor dispatcher,
	                             Timer timer, long period) {
		super(1l);

		Assert.state(timer != null, "Timer must be supplied");
		this.periodTask = new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				if (upstreamSubscription != null) {
					try {
						upstreamSubscription.request(1);
					} catch (InsufficientCapacityException e) {
						//IGNORE
					}
				}
			}
		};

		this.timer = timer;
		this.period = period;
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
		synchronized (this) {
			if (pending != Long.MAX_VALUE) {
				pending--;
			}
		}
		if (pending > 0l) {
			timeoutRegistration = timer.submit(periodTask, period, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public void requestMore(long n) {
		synchronized (this) {
			if (pending != Long.MAX_VALUE) {
				pending += n;
				pending = pending < 0l ? Long.MAX_VALUE : pending;
			}
		}
		if (timeoutRegistration == null) {
			timeoutRegistration = timer.submit(periodTask, period, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public boolean isExposedToOverflow(Bounded upstream) {
		return true;
	}

	@Override
	protected void doShutdown() {
		if(timeoutRegistration != null) {
			timeoutRegistration.cancel();
		}
		super.doShutdown();
	}
}
