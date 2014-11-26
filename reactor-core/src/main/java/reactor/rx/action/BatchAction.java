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

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.event.registry.Registration;
import reactor.function.Consumer;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.support.WrappedSubscription;
import reactor.timer.Timer;

import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public abstract class BatchAction<T, V> extends Action<T, V> {

	protected final boolean                                next;
	protected final boolean                                flush;
	protected final boolean                                first;
	protected final int                                    batchSize;
	protected final Consumer<Long>                         timeoutTask;
	protected final Registration<? extends Consumer<Long>> timespanRegistration;
	protected final Consumer<T> flushConsumer = new FlushConsumer();

	protected int count = 0;

	public BatchAction(
			Dispatcher dispatcher, int batchSize, boolean next, boolean first, boolean flush) {
		this(dispatcher, batchSize, next, first, flush, -1l, null, null);
	}

	public BatchAction(Dispatcher dispatcher, int batchSize, boolean next, boolean first, boolean flush,
	                   long timespan, TimeUnit unit, Timer timer) {
		super(dispatcher);

		if (timespan > 0) {
			this.timeoutTask = new Consumer<Long>() {
				@Override
				public void accept(Long aLong) {
					dispatch(null, flushConsumer);
				}
			};
			TimeUnit targetUnit = unit != null ? unit : TimeUnit.SECONDS;
			timespanRegistration = timer.schedule(timeoutTask, timespan, targetUnit,
					TimeUnit.MILLISECONDS.convert(timespan, targetUnit));
			timespanRegistration.pause();
		} else {
			this.timeoutTask = null;
			this.timespanRegistration = null;
		}
		this.first = first;
		this.flush = flush;
		this.next = next;
		this.batchSize = batchSize;
	}

	@Override
	protected PushSubscription<T> createTrackingSubscription(Subscription subscription) {
		return new RequestConsumer<T>(subscription, this, flushConsumer, batchSize);
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		super.doSubscribe(subscription);
		if (timespanRegistration != null) timespanRegistration.resume();
	}

	protected void nextCallback(T event) {
	}

	protected void flushCallback(T event) {
	}

	protected void firstCallback(T event) {
	}

	@Override
	protected void doNext(T value) {
		count++;
		if (first && count == 1) {
			firstCallback(value);
		}

		if (next) {
			nextCallback(value);
		}

		if (flush && count % batchSize == 0) {
			flushConsumer.accept(value);
		}
	}

	@Override
	protected void doComplete() {
		flushConsumer.accept(null);
		super.doComplete();
	}

	@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		dispatch(elements, upstreamSubscription);
	}

	@Override
	public void cancel() {
		if (timespanRegistration != null) timespanRegistration.cancel();
		super.cancel();
	}

	final private class FlushConsumer implements Consumer<T> {
		@Override
		public void accept(T n) {
			flushCallback(n);
			count = 0;
		}
	}

	final private static class RequestConsumer<T> extends WrappedSubscription<T> {

		final Consumer<T> flushConsumer;
		final int         batchSize;


		public RequestConsumer(Subscription subscription, Subscriber<T> subscriber, Consumer<T> flushConsumer, int
				batchSize) {
			super(subscription, subscriber);
			this.flushConsumer = flushConsumer;
			this.batchSize = batchSize;
		}

		@Override
		public void request(long n) {
			flushConsumer.accept(null);
			super.request(Math.max(n, batchSize));
		}

		@Override
		@SuppressWarnings("unchecked")
		public void maxCapacity(long n) {
			//If a reactor push subscription, assign batch size to max capacity
			if (PushSubscription.class.isAssignableFrom(subscription.getClass())) {
				((PushSubscription<T>) subscription).maxCapacity(batchSize);
			}
		}
	}

	@Override
	public String toString() {
		return super.toString() + "{" + (timespanRegistration != null ? "timed!" : "") + " batchSize=" + count + "/" +
				batchSize + " [" + (int) ((((float) count) / ((float) batchSize)) * 100) + "%]";
	}
}
