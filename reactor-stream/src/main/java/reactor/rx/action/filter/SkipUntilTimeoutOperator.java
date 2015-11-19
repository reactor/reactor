/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.rx.action.filter;

import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Consumer;
import reactor.fn.timer.Timer;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class SkipUntilTimeoutOperator<T> implements Publishers.Operator<T, T> {

	private final long     time;
	private final TimeUnit unit;
	private final Timer    timer;

	public SkipUntilTimeoutOperator(long time, TimeUnit unit, Timer timer) {
		this.time = time;
		this.unit = unit;
		this.timer = timer;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new SkipUntilTimeoutAction<>(subscriber, time, unit, timer);
	}

	static final class SkipUntilTimeoutAction<T> extends SubscriberBarrier<T, T> {

		private final long     time;
		private final TimeUnit unit;
		private final Timer    timer;

		private volatile boolean started = false;

		public SkipUntilTimeoutAction(Subscriber<? super T> actual, long time, TimeUnit unit, Timer timer) {
			super(actual);
			this.unit = unit;
			this.timer = timer;
			this.time = time;
		}

		@Override
		protected void doNext(T ev) {
			if (started) {
				subscriber.onNext(ev);
			}
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			subscriber.onSubscribe(this);
			timer.submit(new Consumer<Long>() {
				@Override
				public void accept(Long aLong) {
					started = true;
				}
			}, time, unit);
		}

		@Override
		public String toString() {
			return super.toString() + "{" +
					"time=" + time +
					"unit=" + unit +
					'}';
		}
	}

}
