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

package reactor.rx.stream;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.InsufficientCapacityException;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.timer.Timer;
import reactor.fn.Consumer;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.5
 */
public final class StreamBufferShiftTimeout<T> extends StreamBarrier<T, List<T>> {

	private final long     timeshift;
	private final long     timespan;
	private final TimeUnit unit;
	private final Timer    timer;

	public StreamBufferShiftTimeout(Publisher<T> source,
			final long timeshift,
			final long timespan,
			TimeUnit unit,
			final Timer timer) {
		super(source);
		if (timespan > 0 && timeshift > 0) {
			final TimeUnit targetUnit = unit != null ? unit : TimeUnit.SECONDS;
			this.timespan = timespan;
			this.timeshift = timeshift;
			this.unit = targetUnit;
			this.timer = timer;
		}
		else {
			this.timespan = -1L;
			this.timeshift = -1L;
			this.unit = null;
			this.timer = null;
		}
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super List<T>> subscriber) {
		return new BufferShiftAction<>(subscriber, timeshift, timespan, unit, timer);
	}

	final static class BufferShiftAction<T> extends SubscriberWithDemand<T, List<T>> {

		private final Consumer<Long> timeshiftTask;
		private final long           timeshift;
		private final TimeUnit       unit;
		private final Timer          timer;

		private final List<List<T>> buckets = new LinkedList<>();

		private Pausable timeshiftRegistration;
		private int      index;

		public BufferShiftAction(Subscriber<? super List<T>> actual,
				final long timeshift,
				final long timespan,
				TimeUnit unit,
				final Timer timer) {

			super(actual);
			if (timespan > 0 && timeshift > 0) {
				final TimeUnit targetUnit = unit != null ? unit : TimeUnit.SECONDS;
				final Consumer<List<T>> flushTimerTask = new Consumer<List<T>>() {
					@Override
					public void accept(List<T> bucket) {
						Iterator<List<T>> it = buckets.iterator();
						while (it.hasNext()) {
							List<T> itBucket = it.next();
							if (bucket == itBucket) {
								it.remove();
								subscriber.onNext(bucket);
								break;
							}
						}
					}
				};

				this.timeshiftTask = new Consumer<Long>() {
					@Override
					public void accept(Long aLong) {
						try {
							if (isTerminated()) {
								return;
							}

							final List<T> bucket = new ArrayList<T>();
							buckets.add(bucket);

							timer.submit(new Consumer<Long>() {
								@Override
								public void accept(Long aLong) {
									flushTimerTask.accept(bucket);
								}
							}, timespan, targetUnit);

						}
						catch (InsufficientCapacityException e) {
							//IGNORE
						}
					}
				};

				this.timeshift = timeshift;
				this.unit = targetUnit;
				this.timer = timer;
			}
			else {
				this.timeshift = -1L;
				this.unit = null;
				this.timer = null;
				this.timeshiftTask = null;
			}
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			subscriber.onSubscribe(this);

			if (timer != null) {
				timeshiftRegistration = timer.schedule(timeshiftTask, timeshift, unit);
			}
		}

		@Override
		protected void doRequested(long before, long n) {
			requestMore(Long.MAX_VALUE);
		}

		@Override
		protected void doNext(T value) {
			flushCallback(value);
		}

		@Override
		protected void checkedError(Throwable ev) {
			buckets.clear();
			subscriber.onError(ev);
		}

		@Override
		protected void checkedComplete() {
			for (List<T> bucket : buckets) {
				subscriber.onNext(bucket);
			}
			buckets.clear();
			subscriber.onComplete();
		}

		@Override
		protected void doTerminate() {
			if (timeshiftRegistration != null) {
				timeshiftRegistration.cancel();
			}
		}

		private void flushCallback(T event) {
			Iterator<List<T>> it = buckets.iterator();
			while (it.hasNext()) {
				List<T> bucket = it.next();
				bucket.add(event);
			}
		}
	}
}
