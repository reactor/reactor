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
package reactor.core.publisher;

import org.reactivestreams.Subscriber;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Function;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A signal interceptor that intercepts all reactive calls and recurse them if a previous signal is still in-flight
 * downstream.
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public final class TrampolineOperator<IN> implements Function<Subscriber<? super IN>, Subscriber<? super IN>> {

	public static final TrampolineOperator INSTANCE = new TrampolineOperator();

	@Override
	public Subscriber<? super IN> apply(Subscriber<? super IN> subscriber) {
		return new TrampolineBarrier<>(subscriber);
	}

	private static class TrampolineBarrier<IN> extends SubscriberBarrier<IN, IN> {

		private final        PriorityBlockingQueue<Task>                  queue           = new PriorityBlockingQueue<>();
		private final        AtomicInteger                                wip             = new AtomicInteger();
		private static final AtomicIntegerFieldUpdater<TrampolineBarrier> COUNTER_UPDATER =
		  AtomicIntegerFieldUpdater.newUpdater
			(TrampolineBarrier.class, "counter");

		@SuppressWarnings("unused")
		private volatile int counter;

		public TrampolineBarrier(Subscriber<? super IN> subscriber) {
			super(subscriber);
		}

		@Override
		protected void doRequest(long n) {
			final Task task = new Task(COUNTER_UPDATER.incrementAndGet(this), n);

			queue.add(task);

			if (wip.getAndIncrement() == 0) {
				do {
					final Task polled = queue.poll();
					if (polled != null) {
						super.doRequest(polled.request);
					}
				} while (wip.decrementAndGet() > 0);
			}
		}

		@Override
		protected void doCancel() {
			queue.clear();
			wip.set(0);
			super.doCancel();
		}


		@Override
		public String toString() {
			return "{trampoline=" + queue.toString() + "}";
		}
	}

	private static class Task implements Comparable<Task> {
		final long request;
		final Long timestamp = System.currentTimeMillis();
		final int index;

		public Task(int index, long request) {
			this.index = index;
			this.request = request;
		}

		@Override
		public int compareTo(Task o) {
			int result = timestamp.compareTo(o.timestamp);
			if (result == 0) {
				return Integer.compare(index, o.index);
			}
			return result;
		}
	}
}
