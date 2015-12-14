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

import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscriber;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.support.Assert;
import reactor.core.support.BackpressureUtils;
import reactor.fn.Consumer;
import reactor.fn.Pausable;
import reactor.core.timer.Timer;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class ThrottleRequestOperator<T> implements Publishers.Operator<T, T> {

	private final Timer timer;
	private final long  period;

	@SuppressWarnings("unchecked")
	public ThrottleRequestOperator(Timer timer, long period) {
		Assert.state(timer != null, "Timer must be supplied");
		this.timer = timer;
		this.period = period;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new ThrottleRequestAction<>(subscriber, timer, period);
	}

	static final class ThrottleRequestAction<T> extends SubscriberWithDemand<T, T> {

		private final Timer          timer;
		private final long           period;
		private final Consumer<Long> periodTask;

		private Pausable timeoutRegistration;

		@SuppressWarnings("unchecked")
		public ThrottleRequestAction(Subscriber<? super T> actual, Timer timer, long period) {
			super(actual);

			Assert.state(timer != null, "Timer must be supplied");
			this.periodTask = new Consumer<Long>() {
				@Override
				public void accept(Long aLong) {
					requestMore(1L);
				}
			};

			this.timer = timer;
			this.period = period;
		}

		@Override
		protected void doNext(T ev) {
			long r = BackpressureUtils.getAndSub(REQUESTED, this, 1L);
			subscriber.onNext(ev);
			if (r != 0L) {
				timeoutRegistration = timer.submit(periodTask, period, TimeUnit.MILLISECONDS);
			}
		}

		@Override
		protected void doRequested(long b, long n) {
			if (timeoutRegistration == null) {
				timeoutRegistration = timer.submit(periodTask, period, TimeUnit.MILLISECONDS);
			}
		}

		@Override
		protected void doTerminate() {
			if (timeoutRegistration != null) {
				timeoutRegistration.cancel();
			}
		}
	}

}
