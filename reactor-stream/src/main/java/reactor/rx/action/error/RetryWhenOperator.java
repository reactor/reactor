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

package reactor.rx.action.error;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.fn.Function;
import reactor.fn.timer.Timer;
import reactor.rx.Stream;
import reactor.rx.action.control.TrampolineOperator;
import reactor.rx.broadcast.Broadcaster;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class RetryWhenOperator<T> implements Publishers.Operator<T, T> {

	private final Timer                                                                 timer;
	private final Publisher<? extends T>                                                rootPublisher;
	private final Function<? super Stream<? extends Throwable>, ? extends Publisher<?>> predicate;

	public RetryWhenOperator(Timer timer,
			Function<? super Stream<? extends Throwable>, ? extends Publisher<?>> predicate,
			Publisher<? extends T> rootPublisher) {

		this.rootPublisher = TrampolineOperator.create(rootPublisher);
		this.predicate = predicate;
		this.timer = timer;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new RetryWhenAction<>(subscriber, timer, predicate, rootPublisher);
	}

	static final class RetryWhenAction<T> extends SubscriberWithDemand<T, T> implements ReactiveState.FeedbackLoop {

		private final Broadcaster<Throwable> retryStream;
		private final Publisher<? extends T> rootPublisher;

		public RetryWhenAction(Subscriber<? super T> actual,
				Timer timer,
				Function<? super Stream<? extends Throwable>, ? extends Publisher<?>> predicate,
				Publisher<? extends T> rootPublisher) {

			super(actual);

			this.retryStream = Broadcaster.create(timer);
			this.rootPublisher = rootPublisher;
			Publisher<?> afterRetryPublisher = predicate.apply(retryStream);
			afterRetryPublisher.subscribe(new RestartSubscriber());
		}

		@Override
		protected void checkedComplete() {
			retryStream.onComplete();
			subscriber.onComplete();
		}

		protected void doRetry() {
			subscription = null;
			rootPublisher.subscribe(RetryWhenAction.this);
		}

		@Override
		protected void doNext(T t) {
			BackpressureUtils.getAndSub(REQUESTED, this, 1L);
			subscriber.onNext(t);
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			if(TERMINATED.compareAndSet(this, TERMINATED_WITH_ERROR, NOT_TERMINATED)) {
				requestMore(BackpressureUtils.addOrLongMax(requestedFromDownstream(), 1L));
			}
			else {
				subscriber.onSubscribe(this);
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		protected void checkedError(Throwable cause) {
			retryStream.onNext(cause);
		}

		@Override
		public Object delegateInput() {
			return retryStream;
		}

		@Override
		public Object delegateOutput() {
			return null;
		}

		private class RestartSubscriber implements Subscriber<Object>, Bounded,  Inner, FeedbackLoop{

			Subscription s;

			@Override
			public long getCapacity() {
				return RetryWhenAction.this.getCapacity();
			}

			@Override
			public void onSubscribe(Subscription s) {
				this.s = s;
				s.request(1L);
			}

			@Override
			public void onNext(Object o) {
				//s.cancel();
				//publisher.subscribe(this);
				doRetry();
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
				cancel();
				subscriber.onComplete();
			}

			@Override
			public Object delegateInput() {
				return RetryWhenAction.this;
			}

			@Override
			public Object delegateOutput() {
				return RetryWhenAction.this;
			}
		}
	}

}
