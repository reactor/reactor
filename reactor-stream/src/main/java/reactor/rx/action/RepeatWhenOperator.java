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

package reactor.rx.action;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.fn.Function;
import reactor.core.timer.Timer;
import reactor.rx.Stream;
import reactor.rx.broadcast.Broadcaster;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class RepeatWhenOperator<T> implements Publishers.Operator<T, T> {

	private final Timer                                                            timer;
	private final Publisher<? extends T>                                           rootPublisher;
	private final Function<? super Stream<? extends Long>, ? extends Publisher<?>> predicate;

	public RepeatWhenOperator(Timer timer,
			Function<? super Stream<? extends Long>, ? extends Publisher<?>> predicate,
			Publisher<? extends T> rootPublisher) {

		this.rootPublisher = rootPublisher != null ? TrampolineOperator.create(rootPublisher) : null;
		this.predicate = predicate;
		this.timer = timer;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new RepeatWhenAction<>(subscriber, timer, predicate, rootPublisher);
	}

	static final class RepeatWhenAction<T> extends SubscriberWithDemand<T, T> {

		private final Broadcaster<Long>      retryStream;
		private final Publisher<? extends T> rootPublisher;

		public RepeatWhenAction(Subscriber<? super T> actual,
				Timer timer,
				Function<? super Stream<? extends Long>, ? extends Publisher<?>> predicate,
				Publisher<? extends T> rootPublisher) {

			super(actual);
			this.retryStream = Broadcaster.create(timer);
			this.rootPublisher = rootPublisher;

			Publisher<?> afterRetryPublisher = predicate.apply(retryStream);
			afterRetryPublisher.subscribe(new RestartSubscriber());
		}

		@Override
		protected void doNext(T ev) {
			BackpressureUtils.getAndSub(REQUESTED, this, 1L);
			subscriber.onNext(ev);
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			if (TERMINATED.compareAndSet(this, TERMINATED_WITH_SUCCESS, NOT_TERMINATED)) {
				long r = requestedFromDownstream();
				if( r > 0L ){
					requestMore(r);
				}
			}
			else {
				subscriber.onSubscribe(this);
			}
		}

		protected void doRetry() {
			subscription = null;
			rootPublisher.subscribe(RepeatWhenAction.this);
		}

		@Override
		protected void checkedComplete() {
			retryStream.onNext(System.currentTimeMillis());
		}

		private class RestartSubscriber implements Subscriber<Object>, ReactiveState.Bounded {

			Subscription s;

			@Override
			public long getCapacity() {
				return RepeatWhenAction.this.getCapacity();
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
				s.request(1L);
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
		}
	}
}
