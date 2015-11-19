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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.fn.Supplier;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class BufferShiftWhenOperator<T> implements Publishers.Operator<T, List<T>> {

	private final Supplier<? extends Publisher<?>> bucketClosing;
	private final Publisher<?>                     bucketOpening;

	public BufferShiftWhenOperator(Publisher<?> bucketOpenings, Supplier<? extends Publisher<?>> boundarySupplier) {
		this.bucketClosing = boundarySupplier;
		this.bucketOpening = bucketOpenings;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super List<T>> subscriber) {
		return new BufferShiftWhenAction<>(subscriber, bucketOpening, bucketClosing);
	}

	static final class BufferShiftWhenAction<T> extends SubscriberWithDemand<T, List<T>> {

		private final List<List<T>> buckets = new LinkedList<>();
		private final Supplier<? extends Publisher<?>> bucketClosing;
		private final Publisher<?>                     bucketOpening;

		public BufferShiftWhenAction(Subscriber<? super List<T>> actual, Publisher<?> bucketOpenings, Supplier<?
				extends Publisher<?>> boundarySupplier) {
			super(actual);
			this.bucketClosing = boundarySupplier;
			this.bucketOpening = bucketOpenings;
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
					List<T> newBucket = new ArrayList<T>();
					synchronized (buckets) {
						buckets.add(newBucket);
					}
					bucketClosing.get()
					             .subscribe(new BucketConsumer(newBucket));

					Subscription s = this.s;
					if (s != null) {
						s.request(1);
					}
				}

				@Override
				public void onError(Throwable t) {
					s = null;
					cancel();
					subscriber.onError(t);
				}

				@Override
				public void onComplete() {
					s = null;
					cancel();
				}
			});

		}

		@Override
		protected void doNext(T value) {
			if (!buckets.isEmpty()) {
				for (List<T> bucket : buckets) {
					bucket.add(value);
				}
			}
		}

		@Override
		protected void checkedError(Throwable ev) {
			buckets.clear();
			subscriber.onError(ev);
		}

		@Override
		protected void checkedComplete() {
			if (!buckets.isEmpty()) {
				for (List<T> bucket : buckets) {
					subscriber.onNext(bucket);
				}
			}
			buckets.clear();
			subscriber.onComplete();
		}

		private class BucketConsumer implements Subscriber<Object> {

			final List<T> bucket;

			public BucketConsumer(List<T> bucket) {
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
				boolean emit;
				synchronized (buckets) {
					emit = buckets.remove(bucket);
				}
				if (emit) {
					subscriber.onNext(bucket);
				}
			}
		}
	}
}
