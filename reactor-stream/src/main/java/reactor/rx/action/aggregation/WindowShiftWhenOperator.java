/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.fn.Supplier;
import reactor.core.timer.Timer;
import reactor.rx.Stream;

/**
 * WindowAction is forwarding events on a steam until {@param backlog} is reached, after that streams collected events
 * further, complete it and create a fresh new stream.
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class WindowShiftWhenOperator<T> implements Publishers.Operator<T, Stream<T>> {

	private final Supplier<? extends Publisher<?>> bucketClosing;
	private final Publisher<?>                     bucketOpening;
	private final Timer                            timer;

	public WindowShiftWhenOperator(Timer timer,
			Publisher<?> bucketOpenings,
			Supplier<? extends Publisher<?>> boundarySupplier) {
		this.bucketClosing = boundarySupplier;
		this.bucketOpening = bucketOpenings;
		this.timer = timer;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super Stream<T>> subscriber) {
		return new WindowShiftWhenAction<>(subscriber, timer, bucketOpening, bucketClosing);
	}

	static final class WindowShiftWhenAction<T> extends SubscriberWithDemand<T, Stream<T>> {

		private final List<WindowOperator.Window<T>> currentWindows = new LinkedList<>();
		private final Supplier<? extends Publisher<?>> bucketClosing;
		private final Publisher<?>                     bucketOpening;
		private final Timer                            timer;

		public WindowShiftWhenAction(Subscriber<? super Stream<T>> actual,
				Timer timer,
				Publisher<?> bucketOpenings,
				Supplier<? extends Publisher<?>> boundarySupplier) {
			super(actual);

			this.bucketClosing = boundarySupplier;
			this.bucketOpening = bucketOpenings;
			this.timer = timer;
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			subscriber.onSubscribe(this);

			bucketOpening.subscribe(new Subscriber<Object>() {
				Subscription s;

				@Override
				public void onSubscribe(Subscription s) {
					this.s = s;
					s.request(1L);
				}

				@Override
				public void onNext(Object o) {

					WindowOperator.Window<T> newBucket = createWindowStream();
					bucketClosing.get()
					             .subscribe(new BucketConsumer(newBucket));

					if (s != null) {
						s.request(1L);
					}
				}

				@Override
				public void onError(Throwable t) {
					cancel();
					subscriber.onError(t);
				}

				@Override
				public void onComplete() {
					s = null;
					if(!isTerminated()){
						createWindowStream();
					}
				}
			});

		}

		@Override
		protected void checkedError(Throwable ev) {
			for (WindowOperator.Window<T> bucket : currentWindows) {
				bucket.onError(ev);
			}
			currentWindows.clear();
			subscriber.onError(ev);
		}

		@Override
		protected void checkedComplete() {
			for (WindowOperator.Window<T> bucket : currentWindows) {
				bucket.onComplete();
			}
			currentWindows.clear();
			subscriber.onComplete();
		}

		@Override
		protected void doNext(T value) {
			if (!currentWindows.isEmpty()) {
				for (WindowOperator.Window<T> bucket : currentWindows) {
					bucket.onNext(value);
				}
			}
		}

		private class BucketConsumer implements Subscriber<Object> {

			final WindowOperator.Window<T> bucket;

			public BucketConsumer(WindowOperator.Window<T> bucket) {
				this.bucket = bucket;
			}

			@Override
			public void onSubscribe(Subscription s) {
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(Object o) {
				onComplete();
			}

			@Override
			public void onError(Throwable t) {
				subscriber.onError(t);
			}

			@Override
			public void onComplete() {
				WindowOperator.Window<T> toComplete = null;

				synchronized (currentWindows) {
					Iterator<WindowOperator.Window<T>> iterator = currentWindows.iterator();
					while (iterator.hasNext()) {
						WindowOperator.Window<T> itBucket = iterator.next();
						if (itBucket == bucket) {
							iterator.remove();
							toComplete = bucket;
							break;
						}
					}
				}

				if (toComplete != null) {
					toComplete.onComplete();
				}
			}
		}

		protected WindowOperator.Window<T> createWindowStream() {
			WindowOperator.Window<T> action = new WindowOperator.Window<T>(timer);
			synchronized (currentWindows) {
				currentWindows.add(action);
			}

			subscriber.onNext(action);
			return action;
		}
	}
}
