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

package reactor.core.publisher.convert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.publisher.PublisherFactory;
import reactor.core.publisher.ValuePublisher;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.core.support.BackpressureUtils;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;

/**
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public final class CompletableFutureConverter
		extends PublisherConverter<CompletableFuture> {

	@Override
	public CompletableFuture fromPublisher(Publisher<?> pub, Class<?> o2) {
		if (CompletableFuture.class.isAssignableFrom(o2)) {
			final AtomicReference<Subscription> ref = new AtomicReference<>();
			final CompletableFuture<List<Object>> future =
					new CompletableFuture<List<Object>>() {
						@Override
						public boolean cancel(boolean mayInterruptIfRunning) {
							boolean cancelled = super.cancel(mayInterruptIfRunning);
							if (cancelled) {
								Subscription s = ref.getAndSet(null);
								if(s != null) {
									s.cancel();
								}
							}
							return cancelled;
						}
					};

			pub.subscribe(new Subscriber<Object>() {
				private List<Object> values = null;

				@Override
				public void onSubscribe(Subscription s) {
					if(BackpressureUtils.checkSubscription(ref.getAndSet(s), s)) {
						s.request(Long.MAX_VALUE);
					}
					else{
						s.cancel();
					}
				}

				@Override
				public void onNext(Object t) {
					if(values == null){
						values = new ArrayList<>();
					}
					values.add(t);
				}

				@Override
				public void onError(Throwable t) {
					if(ref.getAndSet(null) != null) {
						future.completeExceptionally(t);
					}
				}

				@Override
				public void onComplete() {
					if(ref.getAndSet(null) != null) {
						future.complete(values);
					}
				}
			});
			return future;
		}
		return null;
	}

	@Override
	public Publisher<?> toPublisher(Object future) {
		return new CompletableFuturePublisher<>((CompletableFuture<?>) future);
	}

	@Override
	public Class<CompletableFuture> get() {
		return CompletableFuture.class;
	}

	private static class CompletableFuturePublisher<T> implements Publisher<T>,
	                                                              Consumer<Void>,
	                                                              BiConsumer<Long, SubscriberWithContext<T, Void>>{

		private final CompletableFuture<? extends T> future;
		private final Publisher<? extends T>         futurePublisher;

		@SuppressWarnings("unused")
		private volatile long requested;
		private static final AtomicLongFieldUpdater<CompletableFuturePublisher>
				REQUESTED =
				AtomicLongFieldUpdater.newUpdater(CompletableFuturePublisher.class, "requested");

		public CompletableFuturePublisher(CompletableFuture<? extends T> future) {
			this.future = future;
			this.futurePublisher = PublisherFactory.createWithDemand(this, null, this);
		}

		@Override
		public void accept(Long n, final SubscriberWithContext<T, Void> sub) {
			if (!BackpressureUtils.checkRequest(n, sub)) {
				return;
			}

			if (BackpressureUtils.getAndAdd(REQUESTED, CompletableFuturePublisher.this, n) > 0) {
				return;
			}

			future.whenComplete(new java.util.function.BiConsumer<T, Throwable>() {
				@Override
				public void accept(T result, Throwable error) {
					if (error != null) {
						sub.onError(error);
					}
					else {
						sub.onNext(result);
						sub.onComplete();
					}
				}
			});
		}

		@Override
		public void accept(Void aVoid) {
			if (!future.isDone()) {
				future.cancel(true);
			}
		}

		@Override
		public void subscribe(final Subscriber<? super T> subscriber) {
			try {
				if (future.isDone()) {
					new ValuePublisher<>(future.get()).subscribe(subscriber);
				}
				else if (future.isCancelled()) {
					Exceptions.publisher(CancelException.get());
				}
				else {
					futurePublisher.subscribe(subscriber);
				}
			}
			catch (Throwable throwable) {
				Exceptions.<T>publisher(throwable).subscribe(subscriber);
			}
		}
	}
}