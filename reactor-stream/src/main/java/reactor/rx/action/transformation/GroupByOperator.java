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

package reactor.rx.action.transformation;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscriber;
import reactor.Publishers;
import reactor.core.processor.BaseProcessor;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.support.Assert;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.rx.stream.GroupedStream;
import reactor.rx.subscription.ReactiveSubscription;

/**
 * Manage a dynamic registry of substreams for a given key extracted from the incoming data. Each non-existing key will
 * result in a new stream to be signaled
 * @since 2.0, 2.1
 */
public final class GroupByOperator<T, K> implements Publishers.Operator<T, GroupedStream<K, T>> {

	private final Function<? super T, ? extends K> fn;
	private final Timer                            timer;

	public GroupByOperator(Function<? super T, ? extends K> fn, Timer timer) {
		this.fn = fn;
		this.timer = timer;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super GroupedStream<K, T>> subscriber) {
		return new GroupByAction<>(subscriber, timer, fn);
	}

	static final class GroupByAction<T, K> extends SubscriberWithDemand<T, GroupedStream<K, T>> {

		private final Function<? super T, ? extends K> fn;
		private final Timer                            timer;

		@SuppressWarnings("unused")
		private volatile     int                                      running = 0;
		private static final AtomicIntegerFieldUpdater<GroupByAction> RUNNING =
				AtomicIntegerFieldUpdater.newUpdater(GroupByAction.class, "running");

		private final Map<K, ReactiveSubscription<T>> groupByMap = new ConcurrentHashMap<>();

		public GroupByAction(Subscriber<? super GroupedStream<K, T>> actual,
				Timer timer,
				Function<? super T, ? extends K> fn) {
			super(actual);
			Assert.notNull(fn, "Key mapping function cannot be null.");
			this.fn = fn;
			this.timer = timer;
			REQUESTED.lazySet(this, BaseProcessor.SMALL_BUFFER_SIZE);
		}

		public Map<K, ReactiveSubscription<T>> groupByMap() {
			return groupByMap;
		}

		@Override
		protected void doNext(final T value) {
			final K key = fn.apply(value);
			ReactiveSubscription<T> child = groupByMap.get(key);
			if (child == null) {
				child = new ReactiveSubscription<T>(null, null);
				child.getBuffer()
				     .add(value);
				groupByMap.put(key, child);

				final Queue<T> queue = child.getBuffer();
				GroupedStream<K, T> action = new GroupedStream<K, T>(key) {

					@Override
					public long getCapacity() {
						return GroupByAction.this.getCapacity();
					}

					@Override
					public Timer getTimer() {
						return timer;
					}

					@Override
					public void subscribe(Subscriber<? super T> s) {
						final AtomicBoolean last = new AtomicBoolean();
						ReactiveSubscription<T> finalSub = new ReactiveSubscription<T>(this, s, queue) {

							@Override
							public void cancel() {
								super.cancel();
								if (last.compareAndSet(false, true)) {
									removeGroupedStream(key);
								}
							}

							@Override
							public void onComplete() {
								super.onComplete();
								if (last.compareAndSet(false, true)) {
									removeGroupedStream(key);
								}
							}

							@Override
							protected void onRequest(long n) {
								doRequest(n);
							}
						};
						//finalSub.maxCapacity(capacity);
						groupByMap.put(key, finalSub);
						s.onSubscribe(finalSub);
					}
				};
				subscriber.onNext(action);
			}
			else {
				child.onNext(value);
			}
		}

		void drainRequests() {
			int missed = 1;
			long r;
			for (; ; ) {
				r = REQUESTED.getAndSet(this, 0);
				if (r == Long.MAX_VALUE) {
					requestMore(Long.MAX_VALUE);
					return;
				}

				if (r != 0L) {
					requestMore(r);
				}

				missed = RUNNING.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		protected void doRequested(long before, long n) {
			if (RUNNING.getAndIncrement(this) == 0) {
				drainRequests();
			}
		}

		private void removeGroupedStream(K key) {
			ReactiveSubscription<T> innerSub = groupByMap.remove(key);
			if (innerSub != null && groupByMap.isEmpty() && isTerminated()) {
				if (innerSub.isComplete()) {
					subscriber.onComplete();
				}
			}
		}

		@Override
		protected void checkedCancel() {
			for (ReactiveSubscription<T> stream : groupByMap.values()) {
				stream.onComplete();
			}
			super.checkedCancel();
		}

		@Override
		protected void checkedError(Throwable ev) {
			subscriber.onError(ev);
			for (ReactiveSubscription<T> stream : groupByMap.values()) {
				stream.onComplete();
			}
		}

		@Override
		protected void checkedComplete() {
			for (ReactiveSubscription<T> stream : groupByMap.values()) {
				stream.onComplete();
			}
			subscriber.onComplete();
		}
	}
}
