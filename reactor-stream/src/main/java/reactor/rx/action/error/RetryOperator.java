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

package reactor.rx.action.error;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.support.BackpressureUtils;
import reactor.fn.Predicate;
import reactor.rx.action.control.TrampolineOperator;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class RetryOperator<T> implements Publishers.Operator<T, T> {

	private final int                    numRetries;
	private final Predicate<Throwable>   retryMatcher;
	private final Publisher<? extends T> rootPublisher;

	public RetryOperator(int numRetries, Predicate<Throwable> predicate, Publisher<? extends T> parentStream) {
		this.numRetries = numRetries;
		this.retryMatcher = predicate;
		this.rootPublisher = parentStream != null ? TrampolineOperator.create(parentStream) : null;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new RetryAction<>(subscriber, numRetries, retryMatcher, rootPublisher);
	}

	static final class RetryAction<T> extends SubscriberWithDemand<T, T> {

		private final long                   numRetries;
		private final Predicate<Throwable>   retryMatcher;
		private final Publisher<? extends T> rootPublisher;

		private long currentNumRetries = 0;

		public RetryAction(Subscriber<? super T> actual,
				int numRetries,
				Predicate<Throwable> predicate,
				Publisher<? extends T> parentStream) {
			super(actual);

			this.numRetries = numRetries;
			this.retryMatcher = predicate;
			this.rootPublisher = parentStream;
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			if(TERMINATED.compareAndSet(this, TERMINATED_WITH_ERROR, NOT_TERMINATED)) {
				currentNumRetries = 0;
				requestMore(BackpressureUtils.addOrLongMax(getRequested(), 1L));
			}
			else {
				subscriber.onSubscribe(this);
			}
		}

		@Override
		protected void doNext(T ev) {
			BackpressureUtils.getAndSub(REQUESTED, this, 1L);
			subscriber.onNext(ev);
		}

		@Override
		@SuppressWarnings("unchecked")
		protected void checkedError(Throwable throwable) {

			if ((numRetries != -1 && ++currentNumRetries > numRetries) && (retryMatcher == null || !retryMatcher.test(
					throwable))) {
				subscriber.onError(throwable);
				currentNumRetries = 0;
			}
			else if (rootPublisher != null) {
				subscription = null;
				rootPublisher.subscribe(RetryAction.this);
			}
		}
	}

}
