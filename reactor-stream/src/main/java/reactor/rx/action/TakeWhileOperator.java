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

import org.reactivestreams.Subscriber;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Predicate;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class TakeWhileOperator<T> implements Publishers.Operator<T, T> {

	private final Predicate<T> endPredicate;

	public TakeWhileOperator(Predicate<T> endPredicate) {
		this.endPredicate = endPredicate;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new TakeWhileAction<>(subscriber, endPredicate);
	}

	static final class TakeWhileAction<T> extends SubscriberBarrier<T, T> {

		private final Predicate<T> endPredicate;

		public TakeWhileAction(Subscriber<? super T> actual, Predicate<T> predicate) {
			super(actual);
			this.endPredicate = predicate;
		}

		@Override
		protected void doNext(T ev) {
			if (endPredicate != null && !endPredicate.test(ev)) {
				cancel();
				subscriber.onComplete();
			}
			else {
				subscriber.onNext(ev);
			}

		}

		@Override
		public String toString() {
			return super.toString() + "{" +
					"with-end-predicate" +
					'}';
		}
	}

}
