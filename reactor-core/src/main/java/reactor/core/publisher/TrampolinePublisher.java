/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Function;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * A logging interceptor that intercepts all reactive calls and trace them
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public final class TrampolinePublisher<IN> implements Publisher<IN> {

	/**
	 * @param publisher
	 * @param <IN>
	 * @return
	 */
	public static <IN> Publisher<IN> trampoline(Publisher<? extends IN> publisher) {
		return new TrampolinePublisher<>(publisher);
	}

	private final Publisher<IN> wrappedPublisher;

	protected TrampolinePublisher(final Publisher<? extends IN> source) {

		this.wrappedPublisher = PublisherFactory.intercept(
		  source,
		  new Function<Subscriber<? super IN>, SubscriberBarrier<IN, IN>>() {
			  @Override
			  public SubscriberBarrier<IN, IN> apply(Subscriber<? super IN> subscriber) {
				  return new TrampolineBarrier<>(subscriber);
			  }
		  });
	}

	@Override
	public void subscribe(Subscriber<? super IN> s) {
		wrappedPublisher.subscribe(s);
	}

	private static class TrampolineBarrier<IN> extends SubscriberBarrier<IN, IN> {

		private final        PriorityBlockingQueue<Task>                  queue           = new
		  PriorityBlockingQueue<Task>();
		private final        AtomicInteger                                wip             = new AtomicInteger();
		private static final AtomicIntegerFieldUpdater<TrampolineBarrier> COUNTER_UPDATER =
		  AtomicIntegerFieldUpdater.newUpdater
			(TrampolineBarrier.class, "counter");

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
			return super.toString() + "{trampoline=" + queue.toString() + "}";
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
