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

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Processors;
import reactor.core.error.Exceptions;
import reactor.core.processor.FluxProcessor;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.support.Assert;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.core.support.rb.disruptor.RingBuffer;
import reactor.core.timer.Timer;
import reactor.fn.Function;

/**
 * Manage a dynamic registry of substreams for a given key extracted from the incoming data. Each non-existing key will
 * result in a new stream to be signaled
 * @since 2.0, 2.5
 */
public final class StreamGroupBy<T, K> extends StreamBarrier<T, GroupedStream<K, T>> {

	private final Function<? super T, ? extends K> fn;
	private final Timer                            timer;

	public StreamGroupBy(Publisher<T> source, Function<? super T, ? extends K> fn, Timer timer) {
		super(source);
		this.fn = fn;
		this.timer = timer;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super GroupedStream<K, T>> subscriber) {
		return new GroupByAction<>(subscriber, timer, fn);
	}

	static final class GroupedEmitter<T, K> extends GroupedStream<K, T>
			implements Subscription, Subscriber<T>,
			           ReactiveState.Upstream,
			           ReactiveState.Downstream,
			           ReactiveState.Buffering,
			           ReactiveState.ActiveUpstream,
			           ReactiveState.ActiveDownstream,
			           ReactiveState.Inner {

		private final GroupByAction<T, K> parent;
		private final FluxProcessor<T, T> processor;
		//private  Subscriber<? super T> processor;

		@SuppressWarnings("unused")
		private volatile       int                                       terminated = 0;
		@SuppressWarnings("rawtypes")
		protected static final AtomicIntegerFieldUpdater<GroupedEmitter> TERMINATED =
				AtomicIntegerFieldUpdater.newUpdater(GroupedEmitter.class, "terminated");

		@SuppressWarnings("unused")
		private volatile       long                                   requested = 0L;
		@SuppressWarnings("rawtypes")
		protected static final AtomicLongFieldUpdater<GroupedEmitter> REQUESTED =
				AtomicLongFieldUpdater.newUpdater(GroupedEmitter.class, "requested");

		@SuppressWarnings("unused")
		private volatile     int                                       running = 0;
		private static final AtomicIntegerFieldUpdater<GroupedEmitter> RUNNING =
				AtomicIntegerFieldUpdater.newUpdater(GroupedEmitter.class, "running");

		volatile boolean done;
		volatile boolean cancelled;

		volatile Throwable error;

		private volatile Queue<T> buffer;

		public GroupedEmitter(K key, GroupByAction<T, K> parent) {
			super(key);
			this.parent = parent;
			this.processor = Processors.replay(ReactiveState.SMALL_BUFFER_SIZE, Integer.MAX_VALUE, true);
		}


		Queue<T> getBuffer() {
		Queue<T> q = buffer;
			if (q == null) {
				q = RingBuffer.newSequencedQueue(RingBuffer.<T>createSingleProducer(ReactiveState
						.SMALL_BUFFER_SIZE));
				buffer = q;
			}
			return q;
		}

		@Override
		public void request(long n) {
			BackpressureUtils.getAndAdd(REQUESTED, this, n);
			if (RUNNING.getAndIncrement(this) == 0) {
				drainRequests();
			}
		}

		@Override
		public void cancel() {
			if (TERMINATED.compareAndSet(this, 0, 1)) {
				cancelled = true;
				removeGroup();
			}
		}

		void start() {
			processor.onSubscribe(this);
		}

		@Override
		public void subscribe(final Subscriber<? super T> subscriber) {
			processor.subscribe(subscriber);
		}

		@Override
		public void onSubscribe(Subscription s) {
			//IGNORE
		}

		@Override
		public void onNext(T t) {
			Queue<T> buffer = this.buffer;
			if ((buffer == null || buffer.isEmpty()) && BackpressureUtils.getAndSub(REQUESTED, this, 1L) != 0L) {
				processor.onNext(t);
				parent.updateRemaining(1L);
			}
			else {
				GroupByAction.BUFFERED.incrementAndGet(parent);
				getBuffer().add(t);
				if (RUNNING.getAndIncrement(this) == 0) {
					drainRequests();
				}
			}
		}

		@Override
		public void onError(Throwable t) {
			if (TERMINATED.compareAndSet(this, 0, 1)) {
				done = true;
				error = t;
				removeGroup();
				if (RUNNING.getAndIncrement(this) == 0) {
					drainRequests();
				}
			}
		}

		@Override
		public void onComplete() {
			if (TERMINATED.compareAndSet(this, 0, 1)) {
				done = true;
				removeGroup();
				if (RUNNING.getAndIncrement(this) == 0) {
					drainRequests();
				}
			}
		}

		@Override
		public long getCapacity() {
			return ReactiveState.SMALL_BUFFER_SIZE;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isStarted() {
			return !cancelled;
		}

		@Override
		public boolean isTerminated() {
			return done;
		}

		@Override
		public long pending() {
			return buffer != null ? buffer.size() : -1L;
		}

		@Override
		public Object downstream() {
			return processor;
		}

		@Override
		public Object upstream() {
			return parent;
		}

		@Override
		public Timer getTimer() {
			return parent.timer;
		}

		void removeGroup() {
			GroupedEmitter<T, K> g = parent.groupByMap.remove(key());
			if (g != null) {
				Queue<T> buffer = g.buffer;
				int size = buffer != null ? buffer.size() : -1;
				if (size > 0) {
					GroupByAction.BUFFERED.addAndGet(parent, -size);
					parent.checkGroupsCompleted();
					parent.updateRemaining(size);
					return;
				}
				parent.checkGroupsCompleted();
			}
		}

		void drainRequests() {
			int missed = 1;
			long r, produced;
			T v;
			Queue<T> buffer;
			boolean done;
			for (; ; ) {
				done = this.done;
				buffer = this.buffer;
				r = requested;
				produced = 0L;

				for (; ; ) {

					if (r == 0L || buffer == null) {
						break;
					}

					if (cancelled) {
						return;
					}

					v = buffer.poll();

					if (v != null) {
						processor.onNext(v);
						produced++;
						if (r != Long.MAX_VALUE) {
							r--;
						}
					}
					else {
						break;
					}
				}

				if (cancelled) {
					return;
				}

				if (done) {
					if (error != null) {
						processor.onError(error);
						return;
					}
					else if (buffer == null || buffer.isEmpty()) {
						processor.onComplete();
						return;
					}
				}

				if (produced > 0L) {
					GroupByAction.BUFFERED.addAndGet(parent, -produced);
					REQUESTED.addAndGet(this, -produced);
					if (!done) {
						parent.updateRemaining(produced);
					}
				}

				missed = RUNNING.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}

		@Override
		public String toString() {
			return "GroupedEmitter{" +
					"key=" + key() +
					", buffered=" + parent.buffered +
					", requested=" + requested +
					'}';
		}
	}

	static final class GroupByAction<T, K> extends SubscriberWithDemand<T, GroupedStream<K, T>>
			implements ReactiveState.LinkedDownstreams, ReactiveState.Buffering{

		private final Function<? super T, ? extends K> fn;

		private final Timer timer;
		private final int   limit;
		private final ConcurrentHashMap<K, GroupedEmitter<T, K>> groupByMap = new ConcurrentHashMap<>();

		@SuppressWarnings("unused")
		private volatile long                                  buffered         = 0L;
		static final     AtomicLongFieldUpdater<GroupByAction>    BUFFERED          =
				AtomicLongFieldUpdater.newUpdater(GroupByAction.class, "buffered");
		@SuppressWarnings("unused")

		private volatile int                                      actualComplete    = 0;
		static final     AtomicIntegerFieldUpdater<GroupByAction> ACTUAL_COMPLETED  =
				AtomicIntegerFieldUpdater.newUpdater(GroupByAction.class, "actualComplete");
		@SuppressWarnings("unused")

		//include self in total ready cancelled groups
		private volatile int                                      cancellableGroups = 1;
		static final     AtomicIntegerFieldUpdater<GroupByAction> CANCELLED_GROUPS  =
				AtomicIntegerFieldUpdater.newUpdater(GroupByAction.class, "cancellableGroups");

		public GroupByAction(Subscriber<? super GroupedStream<K, T>> actual,
				Timer timer,
				Function<? super T, ? extends K> fn) {
			super(actual);
			Assert.notNull(fn, "Key mapping function cannot be null.");
			this.fn = fn;
			this.timer = timer;
			this.limit = ReactiveState.SMALL_BUFFER_SIZE / 2;
		}

		public Map<K, GroupedEmitter<T, K>> groupByMap() {
			return groupByMap;
		}

		@Override
		protected void doNext(final T value) {
			final K key = fn.apply(value);

			GroupedEmitter<T, K> child = groupByMap.get(key);
			if (child == null) {
				child = new GroupedEmitter<>(key, this);

				GroupedEmitter<T, K> p;

				for (;;) {
					int cancelled = cancellableGroups;
					if (cancelled <= 0) {
						Exceptions.onNextDropped(value);
					}
					if (CANCELLED_GROUPS.compareAndSet(this, cancelled, cancelled + 1)) {
						p = groupByMap.putIfAbsent(key, child);
						break;
					}
				}

				if (p != null) {
					child = p;
				}
				else {
					child.start();
					subscriber.onNext(child);
					child.onNext(value);
					return;
				}
			}

			child.onNext(value);
		}

		protected final void updateRemaining(long n) {
			long remaining = REQUESTED.addAndGet(this, -n);
			long buffered = BUFFERED.get(this);
			if (remaining < limit) {
				long toRequest = ReactiveState.SMALL_BUFFER_SIZE - buffered;
				if (toRequest > 0 && REQUESTED.compareAndSet(this, remaining, remaining + toRequest)) {
					requestMore(toRequest);
				}
			}
		}

		@Override
		protected final void doRequested(long b, long n) {
			if (b == 0) {
				requestMore(n);
			}
		}

		@Override
		public Iterator<?> downstreams() {
			return groupByMap().values().iterator();
		}

		@Override
		public long downstreamsCount() {
			return groupByMap.size();
		}

		@Override
		protected void doCancel() {
			if(CANCELLED_GROUPS.decrementAndGet(this) == 0){
				super.doCancel();
			}
		}

		@Override
		protected void checkedError(Throwable ev) {
			for (GroupedEmitter<T, K> stream : groupByMap.values()) {
				stream.onError(ev);
			}
			subscriber.onError(ev);
		}

		@Override
		protected void checkedComplete() {
			for (GroupedEmitter<T, K> stream : groupByMap.values()) {
				stream.onComplete();
			}

			if (groupByMap.isEmpty() &&
					ACTUAL_COMPLETED.compareAndSet(this, 0, 1)) {
				subscriber.onComplete();
			}
		}

		void checkGroupsCompleted(){
			if(CANCELLED_GROUPS.decrementAndGet(this) == 0){
				super.doCancel();
			}
			else if (isCompleted() &&
					groupByMap.isEmpty() &&
					ACTUAL_COMPLETED.compareAndSet(this, 0, 1)) {
				subscriber.onComplete();
			}
		}

		@Override
		public long pending() {
			return BUFFERED.get(this);
		}

		@Override
		public String toString() {
			return "GroupByAction{" +
					"limit=" + limit +
					", groupByMap=" + groupByMap +
					", buffered=" + buffered +
					", actualComplete=" + actualComplete +
					", cancellableGroups=" + cancellableGroups +
					", requested=" + requestedFromDownstream() +
					", capacity=" + getCapacity() +
					'}';
		}
	}


}
