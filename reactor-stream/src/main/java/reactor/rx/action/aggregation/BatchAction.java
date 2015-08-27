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
package reactor.rx.action.aggregation;

import org.reactivestreams.Subscription;
import reactor.core.support.Bounded;
import reactor.fn.Consumer;
import reactor.fn.Pausable;
import reactor.fn.timer.Timer;
import reactor.rx.action.Action;
import reactor.rx.subscription.BatchSubscription;
import reactor.rx.subscription.PushSubscription;

import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public abstract class BatchAction<T, V> extends Action<T, V> {

	protected final boolean  next;
	protected final boolean  flush;
	protected final boolean  first;
	protected final int      batchSize;
	protected final long     timespan;
	protected final TimeUnit unit;
	protected final Timer    timer;
	protected final Consumer<T> flushConsumer = new FlushConsumer();
	protected final Consumer<Long> flushTask;

	protected int index = 0;
	private Pausable timespanRegistration;

	public BatchAction(
	  int batchSize, boolean next, boolean first, boolean flush) {
		this(batchSize, next, first, flush, -1l, null, null);
	}

	public BatchAction(int batchSize, boolean next, boolean first, boolean flush,
	                   long timespan, TimeUnit unit, Timer timer) {
		super(batchSize);
		if (timespan > 0) {
			this.unit = unit != null ? unit : TimeUnit.SECONDS;
			this.timespan = timespan;
			this.timer = timer;
			this.flushTask = new Consumer<Long>() {
				@Override
				public void accept(Long aLong) {
					if (isPublishing()) {
						flushConsumer.accept(null);
					}
				}
			};
		} else {
			this.timespan = -1L;
			this.timer = null;
			this.flushTask = null;
			this.unit = null;
		}
		this.first = first;
		this.flush = flush;
		this.next = next;
		this.batchSize = batchSize;
		//this.capacity = batchSize;
	}

	@Override
	protected PushSubscription<T> createTrackingSubscription(Subscription subscription) {
		return new BatchSubscription<T>(subscription, this, batchSize);
	}

	@Override
	public boolean isExposedToOverflow(Bounded upstream) {
		return false;
	}

	protected void nextCallback(T event) {
	}

	protected void flushCallback(T event) {
	}

	protected void firstCallback(T event) {
	}

	@Override
	protected void doNext(T value) {

		if (++index == 1) {
			if (timer != null) {
				timespanRegistration = timer.submit(flushTask, timespan, unit);
			}
			if (first) {
				firstCallback(value);
			}
		}

		if (next) {
			nextCallback(value);
		}

		if (index % batchSize == 0) {
			if (timespanRegistration != null) {
				timespanRegistration.cancel();
				timespanRegistration = null;
			}
			index = 0;
			if (flush) {
				if (timer != null) {
					timespanRegistration = timer.submit(flushTask);
				} else {
					flushConsumer.accept(value);
				}
			}
		}
	}

	@Override
	protected void doComplete() {
		flushConsumer.accept(null);
		super.doComplete();
	}


	final private class FlushConsumer implements Consumer<T> {
		@Override
		public void accept(T n) {
			flushCallback(n);
			index = 0;
		}
	}

	@Override
	public String toString() {
		return super.toString() + "{" + (timer != null ? "timed - " + timespan + " " + unit : "") + " batchSize=" +
		  index + "/" +
		  batchSize + " [" + (int) ((((float) index) / ((float) batchSize)) * 100) + "%]";
	}

}
