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

import org.reactivestreams.Subscriber;
import reactor.Environment;
import reactor.ReactorProcessor;
import reactor.core.queue.CompletableQueue;
import reactor.core.subscriber.SerializedSubscriber;
import reactor.core.support.Assert;
import reactor.fn.Function;
import reactor.rx.action.Action;
import reactor.rx.action.support.DefaultSubscriber;
import reactor.rx.stream.GroupedStream;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.ReactiveSubscription;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manage a dynamic registry of substreams for a given key extracted from the incoming data. Each non-existing key
 * will result in a new stream to be signaled
 *
 * @param <T>
 * @param <K>
 * @since 2.0
 */
public class GroupByAction<T, K> extends Action<T, GroupedStream<K, T>> {

	private final Function<? super T, ? extends K> fn;
	private final Environment                      environment;
	private final ReactorProcessor                 dispatcher;

	private final Map<K, ReactiveSubscription<T>> groupByMap = new ConcurrentHashMap<>();
	private final SerializedSubscriber<Long>      serialized = SerializedSubscriber.create(new DefaultSubscriber<Long>
	  () {

		@Override
		public void onNext(Long aLong) {
			checkRequest(aLong);
			if (upstreamSubscription != null) {
				upstreamSubscription.request(aLong);
			}
		}

	});


	public GroupByAction(Environment environment, Function<? super T, ? extends K> fn, ReactorProcessor dispatcher) {
		Assert.notNull(fn, "Key mapping function cannot be null.");
		this.dispatcher = dispatcher;
		this.fn = fn;
		this.environment = environment;
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
			child.getBuffer().add(value);
			groupByMap.put(key, child);

			final CompletableQueue<T> queue = child.getBuffer();
			GroupedStream<K, T> action = new GroupedStream<K, T>(key) {

				@Override
				public long getCapacity() {
					return GroupByAction.this.getCapacity();
				}

				@Override
				public ReactorProcessor getDispatcher() {
					return dispatcher;
				}

				@Override
				public Environment getEnvironment() {
					return environment;
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
							serialized.onNext(n);
						}
					};
					//finalSub.maxCapacity(capacity);
					groupByMap.put(key, finalSub);
					s.onSubscribe(finalSub);
				}
			};
			broadcastNext(action);
		} else {
			child.onNext(value);
		}
	}

	private void removeGroupedStream(K key) {
		PushSubscription<T> parentSub = upstreamSubscription;
		ReactiveSubscription<T> innerSub = groupByMap.remove(key);
		if (innerSub != null
				&& groupByMap.isEmpty() &&
				((parentSub == null || parentSub.isComplete()))) {

			PushSubscription<GroupedStream<K, T>> childSub = downstreamSubscription;
			if (childSub == null || childSub.isComplete()) {
				cancel();
			}

			if (innerSub.getBufferSize() == 0l) {
				broadcastComplete();
			}
		}
	}

	@Override
	protected void doComplete() {
		for (ReactiveSubscription<T> stream : groupByMap.values()) {
			stream.onComplete();
		}

		super.doComplete();
	}

	@Override
	public void requestMore(long n) {
		serialized.onNext(n);
	}

	@Override
	public final ReactorProcessor getDispatcher() {
		return dispatcher;
	}

	@Override
	public final Environment getEnvironment() {
		return environment;
	}

}
