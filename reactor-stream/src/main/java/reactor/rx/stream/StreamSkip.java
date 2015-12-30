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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Predicate;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.5
 */
public final  class StreamSkip<T> extends StreamBarrier<T, T> {

	private final Predicate<T> startPredicate;
	private final long         limit;

	public StreamSkip(Publisher<T> source, Predicate<T> startPredicate, long limit) {
		super(source);
		this.startPredicate = startPredicate;
		this.limit = limit;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new SkipAction<>(subscriber, startPredicate, limit);
	}

	static final class SkipAction<T> extends SubscriberBarrier<T, T> {

		private final Predicate<T> startPredicate;
		private final long         limit;

		private long counted = 0L;

		public SkipAction(Subscriber<? super T> actual, Predicate<T> predicate, long limit) {
			super(actual);
			this.startPredicate = predicate;
			this.limit = limit;
		}

		@Override
		protected void doNext(T ev) {
			if (counted == -1L || counted++ >= limit || (startPredicate != null && !startPredicate.test(ev))) {
				subscriber.onNext(ev);
				if(counted != 1L){
					counted = -1L;
				}
			}
		}


		@Override
		public String toString() {
			return super.toString() + "{" +
					"skip=" + limit + (startPredicate == null ?
					", with-start-predicate" : "") +
					'}';
		}
	}
}
