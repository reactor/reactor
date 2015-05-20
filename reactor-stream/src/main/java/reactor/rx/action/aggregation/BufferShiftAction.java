/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.rx.action.aggregation;

import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.core.dispatch.InsufficientCapacityException;
import reactor.fn.Consumer;
import reactor.fn.Pausable;
import reactor.fn.timer.Timer;
import reactor.rx.action.Action;
import reactor.rx.subscription.BatchSubscription;
import reactor.rx.subscription.PushSubscription;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public final class BufferShiftAction<T> extends Action<T, List<T>> {

	private final List<List<T>> buckets = new LinkedList<>();
	private final Consumer<Long> timeshiftTask;
	private final long           timeshift;
	private final TimeUnit       unit;
	private final Timer          timer;
	private final int            skip;
	private final int            batchSize;

	private Pausable timeshiftRegistration;
	private int      index;

	public BufferShiftAction(Dispatcher dispatcher, int size, int skip) {
		this(dispatcher, size, skip, -1l, -1l, null, null);
	}

	public BufferShiftAction(final Dispatcher dispatcher, int size, int skip,
	                         final long timeshift, final long timespan, TimeUnit unit, final Timer timer) {
		super(size);
		this.skip = skip;
		this.batchSize = size;
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
							broadcastNext(bucket);
							break;
						}
					}
				}
			};

			this.timeshiftTask = new Consumer<Long>() {
				@Override
				public void accept(Long aLong) {
					try {
						if (!isPublishing()) {
							return;
						}

						dispatcher.tryDispatch(null, new Consumer<Void>() {
							@Override
							public void accept(Void aVoid) {
								final List<T> bucket = new ArrayList<T>();
								buckets.add(bucket);

								timer.submit(new Consumer<Long>() {
									@Override
									public void accept(Long aLong) {
										dispatcher.dispatch(bucket, flushTimerTask, null);
									}
								}, timespan, targetUnit);
							}
						}, null);
					} catch (InsufficientCapacityException e) {
						//IGNORE
					}
				}
			};

			this.timeshift = timeshift;
			this.unit = targetUnit;
			this.timer = timer;
		} else {
			this.timeshift = -1l;
			this.unit = null;
			this.timer = null;
			this.timeshiftTask = null;
		}
	}

	@Override
	protected void doOnSubscribe(Subscription subscription) {
		if(timer != null) {
			timeshiftRegistration = timer.schedule(timeshiftTask,
					timeshift,
					unit);
		}
	}

	@Override
	protected PushSubscription<T> createTrackingSubscription(Subscription subscription) {
		return new BatchSubscription<>(subscription, this, skip + batchSize);
	}

	@Override
	protected void doNext(T value) {
		if (timer == null && index++ % skip == 0) {
			buckets.add(batchSize < 2048 ? new ArrayList<T>(batchSize) : new ArrayList<T>());
		}
		flushCallback(value);
	}

	@Override
	protected void doError(Throwable ev) {
		buckets.clear();
		super.doError(ev);
	}

	@Override
	protected void doComplete() {
		for (List<T> bucket : buckets) {
			broadcastNext(bucket);
		}
		buckets.clear();
		super.doComplete();
	}

	private void flushCallback(T event) {
		Iterator<List<T>> it = buckets.iterator();
		while (it.hasNext()) {
			List<T> bucket = it.next();
			bucket.add(event);
			if (bucket.size() == batchSize) {
				it.remove();
				broadcastNext(bucket);
			}
		}
	}

	@Override
	public String toString() {
		return super.toString() + "{skip=" + skip + "}";
	}
}
