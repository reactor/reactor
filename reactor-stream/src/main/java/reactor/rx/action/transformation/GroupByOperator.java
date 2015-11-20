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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Processors;
import reactor.Publishers;
import reactor.core.processor.BaseProcessor;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.support.Assert;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.rx.stream.GroupedStream;

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

		private final Map<K, Processor<T, T>> groupByMap = new ConcurrentHashMap<>();

		public GroupByAction(Subscriber<? super GroupedStream<K, T>> actual,
				Timer timer,
				Function<? super T, ? extends K> fn) {
			super(actual);
			Assert.notNull(fn, "Key mapping function cannot be null.");
			this.fn = fn;
			this.timer = timer;
			REQUESTED.lazySet(this, BaseProcessor.SMALL_BUFFER_SIZE);
		}

		public Map<K, Processor<T, T>> groupByMap() {
			return groupByMap;
		}

		@Override
		protected void doNext(final T value) {
			final K key = fn.apply(value);
			Processor<T, T> child = groupByMap.get(key);
			if (child == null) {
				final BaseProcessor<T, T> fresh = Processors.emitter();
				child = fresh;

				child.onNext(value);

				child.onSubscribe(new Subscription() {
					@Override
					public void cancel() {
						groupByMap.remove(key);
					}

					@Override
					public void request(long n) {
						doRequest(n);
					}
				});

				GroupedStream<K, T> action = new GroupedStream<K, T>(key) {

					@Override
					public long getCapacity() {
						return fresh.getCapacity();
					}

					@Override
					public Timer getTimer() {
						return timer;
					}

					@Override
					public void subscribe(Subscriber<? super T> s) {
						fresh.subscribe(s);
					}
				};

				groupByMap.put(key, child);

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

		@Override
		protected void checkedCancel() {
			for (Processor<T, T> stream : groupByMap.values()) {
				stream.onComplete();
			}
			super.checkedCancel();
		}

		@Override
		protected void checkedError(Throwable ev) {
			subscriber.onError(ev);
			for (Processor<T, T> stream : groupByMap.values()) {
				stream.onComplete();
			}
		}

		@Override
		protected void checkedComplete() {
			for (Processor<T, T> stream : groupByMap.values()) {
				stream.onComplete();
			}
			subscriber.onComplete();
		}

		@Override
		protected void doTerminate() {
			groupByMap.clear();
		}
	}
}
