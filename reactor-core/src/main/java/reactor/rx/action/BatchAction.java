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
import reactor.core.Dispatcher;
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
		//this.capacity = batchSize;
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

		private final Consumer<T> flushConsumer;
		private final int         batchSize;

		public RequestConsumer(Subscription subscription, Subscriber<T> subscriber, Consumer<T> flushConsumer, int
				batchSize) {
			super(subscription, subscriber);
			this.flushConsumer = flushConsumer;
			this.batchSize = batchSize;
		}

		@Override
		public void request(long n) {
			flushConsumer.accept(null);
			if (pushSubscription != null) {
				if (n == Long.MAX_VALUE) {
					pushSubscription.request(Long.MAX_VALUE);
				} else if (pushSubscription.pendingRequestSignals() != Long.MAX_VALUE) {
					if (n > batchSize) {
						pushSubscription.updatePendingRequests(n - batchSize);
						pushSubscription.request(batchSize);
					} else {
						pushSubscription.request(n);
					}
				}
			} else {
					super.request(n);
			}
		}

		@Override
		public boolean shouldRequestPendingSignals() {
			return (pushSubscription != null && (pushSubscription.pendingRequestSignals() % batchSize == 0))
					|| super.shouldRequestPendingSignals();
		}

		@Override
		public void maxCapacity(long n) {
			super.maxCapacity(n);
		}

		@Override
		public long clearPendingRequest() {
			if (pushSubscription != null) {
				long pending = pushSubscription.clearPendingRequest();
				if (pending > batchSize) {
					long toRequest = pending - batchSize;
					pushSubscription.updatePendingRequests(toRequest);
					return batchSize;
				} else {
					return pending;
				}
			} else {
				return super.clearPendingRequest();
			}
		}
	}

	@Override
	public String toString() {
		return super.toString() + "{" + (timespanRegistration != null ? "timed!" : "") + " batchSize=" + count + "/" +
				batchSize + " [" + (int) ((((float) count) / ((float) batchSize)) * 100) + "%]";
	}
}
