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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.support.Assert;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.fn.Function;
import reactor.fn.Supplier;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class PublisherOnErrorResume<T>
		implements Function<Subscriber<? super T>, Subscriber<? super T>>, ReactiveState.Factory {

	private final Function<Throwable, ? extends Publisher<? extends T>> fallbackSelector;

	public PublisherOnErrorResume(final Publisher<? extends T> fallbackSelector) {
		this.fallbackSelector = new Function<Throwable, Publisher<? extends T>>() {
			@Override
			public Publisher<? extends T> apply(Throwable throwable) {
				return fallbackSelector;
			}
		};
	}

	public PublisherOnErrorResume(Function<Throwable, ? extends Publisher<? extends T>> fallbackSelector) {
		this.fallbackSelector = fallbackSelector;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new ErrorSelectBarrier<>(subscriber, fallbackSelector);
	}

	static final class ErrorSelectBarrier<T> extends SubscriberWithDemand<T, T> implements Named {

		private final Function<Throwable, ? extends Publisher<? extends T>> fallbackSelector;

		private volatile FallbackSubscriber<T> fallback;
		private          FallbackSubscriber<T> cachedFallback;

		private Throwable error;

		public ErrorSelectBarrier(Subscriber<? super T> actual,
				Function<Throwable, ? extends Publisher<? extends T>> fallbackSelector) {
			super(actual);
			Assert.notNull(fallbackSelector, "Fallback Selector function cannot be null.");
			this.fallbackSelector = fallbackSelector;
		}

		@Override
		public String getName() {
			return ReactiveStateUtils.getName(fallbackSelector);
		}

		@Override
		public Throwable getError() {
			return error;
		}

		@Override
		protected void doRequest(long n) {
			if (this.cachedFallback == null) {
				this.cachedFallback = this.fallback;
				if (this.cachedFallback != null) {
					this.cachedFallback.request(n);
					return;
				}
			}
			super.doRequest(n);
		}

		@Override
		protected void doCancel() {
			if (this.cachedFallback == null) {
				this.cachedFallback = this.fallback;
				if (this.cachedFallback != null) {
					this.cachedFallback.cancel();
					return;
				}
			}
			super.doCancel();
		}

		@Override
		@SuppressWarnings("unchecked")
		protected void checkedError(Throwable throwable) {
			this.error = throwable;
			Publisher<? extends T> fallback = fallbackSelector.apply(throwable);
			final long r = requestedFromDownstream();
			if (fallback == null) {
				super.checkedError(throwable);
			}
			else if (r != 0 && Supplier.class.isAssignableFrom(fallback.getClass())) {
				subscriber.onNext(((Supplier<T>) fallback).get());
				subscriber.onComplete();
			}
			else {
				FallbackSubscriber<T> s = new FallbackSubscriber<>(subscriber, r);
				this.fallback = s;
				fallback.subscribe(s);
			}
		}

		private static class FallbackSubscriber<T> extends SubscriberBarrier<T, T> {

			private final long initRequest;

			public FallbackSubscriber(Subscriber<? super T> subscriber, long r) {
				super(subscriber);
				this.initRequest = r;
			}

			@Override
			public void doOnSubscribe(Subscription s) {
				if (initRequest != 0L) {
					s.request(initRequest);
				}
			}
		}
	}

}
