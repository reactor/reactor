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
package reactor.rx.action;

import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.event.registry.Registration;
import reactor.function.Consumer;
import reactor.timer.Timer;
import reactor.util.Assert;

import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class ThrottleRequestAction<T> extends Action<T, T> {

	private final Timer timer;
	private final long  period;
	private final Consumer<Long> periodTask = new Consumer<Long>() {
		@Override
		public void accept(Long aLong) {
			if (upstreamSubscription != null) {
				dispatch(1l, upstreamSubscription);
			}
		}
	};

	private final long delay;

	private Registration<? extends Consumer<Long>> timeoutRegistration;

	@SuppressWarnings("unchecked")
	public ThrottleRequestAction(Dispatcher dispatcher,
	                             Timer timer, long period, long delay) {
		super(dispatcher, 1);
		Assert.state(timer != null, "Timer must be supplied");
		this.timer = timer;
		this.period = period;
		this.delay = delay;
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		super.doSubscribe(subscription);
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
	}

	@Override
	public void requestMore(long n) {
		timeoutRegistration = timer.submit(periodTask, period, TimeUnit.MILLISECONDS);
	}

	@Override
	protected void doStart(long pending) {
		requestMore(pending);
	}

	@Override
	public void cancel() {
		if(timeoutRegistration != null) {
			timeoutRegistration.cancel();
		}
		super.cancel();
	}

	@Override
	public void doComplete() {
		if(timeoutRegistration != null) {
			timeoutRegistration.cancel();
		}
		super.doComplete();
	}
}
