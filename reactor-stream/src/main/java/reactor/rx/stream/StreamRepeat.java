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

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Processors;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.core.support.BackpressureUtils;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class StreamRepeat<T> extends StreamBarrier<T, T> {

	private final int                    numRetries;

	public StreamRepeat(Publisher<T> source, int numRetries) {
		super(source);
		this.numRetries = numRetries;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new RepeatAction<>(subscriber, numRetries, source);
	}

	static final class RepeatAction<T> extends SubscriberWithDemand<T, T> {

		private final int                    numRetries;
		private final Publisher<? extends T> rootPublisher;
		private long currentNumRetries = 0;

		public RepeatAction(Subscriber<? super T> actual, int numRetries, Publisher<? extends T> parentStream) {
			super(actual);
			this.numRetries = numRetries;
			this.rootPublisher = parentStream;
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

		@Override
		protected void checkedComplete() {
			if (numRetries != -1 && ++currentNumRetries > numRetries) {
				subscriber.onComplete();
				currentNumRetries = 0;
			}
			else {
				if (rootPublisher != null) {
					subscription = null;
					Processor<T, T> emitter = Processors.emitter();
					emitter.subscribe(RepeatAction.this);
					rootPublisher.subscribe(emitter);
				}
			}
		}
	}
}
