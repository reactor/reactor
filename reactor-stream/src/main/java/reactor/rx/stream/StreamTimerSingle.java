/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
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

package reactor.rx.stream;

import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscriber;
import reactor.core.error.Exceptions;
import reactor.core.support.ReactiveState;
import reactor.core.timer.Timer;
import reactor.fn.Consumer;
import reactor.rx.Stream;
import reactor.rx.subscription.PushSubscription;

/**
 * A Stream that emits {@link 0} after an initial delay and then complete
 * <p>
 * The SingleTimerStream will manage dedicated timers for new subscriber assigned via {@link
 * this#subscribe(org.reactivestreams.Subscriber)}.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * <pre>
 * {@code
 * Streams.timer(env, 1).consume(
 * log::info,
 * log::error,
 * (-> log.info("complete"))
 * )
 * }
 * </pre>
 * Will log:
 * <pre>
 * {@code
 * 0
 * complete
 * }
 * </pre>
 *
 * @author Stephane Maldini
 */
public final class StreamTimerSingle extends Stream<Long> implements ReactiveState.FeedbackLoop {

	final private long     delay;
	final private TimeUnit unit;
	final private Timer    timer;

	public StreamTimerSingle(long delay, TimeUnit unit, Timer timer) {
		this.delay = delay >= 0l ? delay : 0l;
		this.unit = unit != null ? unit : TimeUnit.SECONDS;
		this.timer = timer;
	}

	@Override
	public void subscribe(final Subscriber<? super Long> subscriber) {
		try {
			subscriber.onSubscribe(new TimerSubscription(this, subscriber));
		}
		catch (Throwable throwable) {
			Exceptions.throwIfFatal(throwable);
			subscriber.onError(throwable);
		}
	}

	private class TimerSubscription extends PushSubscription<Long> implements Timed {

		final Pausable registration = timer.submit(new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				subscriber.onNext(0l);
				subscriber.onComplete();
				timer.cancel();
			}
		}, delay, unit);

		public TimerSubscription(Stream<Long> publisher, Subscriber<? super Long> subscriber) {
			super(publisher, subscriber);
		}

		@Override
		public void cancel() {
			registration.cancel();
			timer.cancel();
			super.cancel();
		}

		@Override
		public long period() {
			return TimeUnit.MILLISECONDS.convert(delay, unit);
		}
	}

	@Override
	public Object delegateInput() {
		return timer;
	}

	@Override
	public Object delegateOutput() {
		return timer;
	}

	@Override
	public String toString() {
		return "delay=" + delay + " " + unit;
	}
}
