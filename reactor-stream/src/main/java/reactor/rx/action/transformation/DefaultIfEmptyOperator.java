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

package reactor.rx.action.transformation;

import org.reactivestreams.Subscriber;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberBarrier;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class DefaultIfEmptyOperator<T> implements Publishers.Operator<T, T> {

	private final T defaultValue;

	public DefaultIfEmptyOperator(T defaultValue) {
		this.defaultValue = defaultValue;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new DefaultIfEmptyAction<>(subscriber, defaultValue);
	}

	static final class DefaultIfEmptyAction<T> extends SubscriberBarrier<T, T> {

		private final T defaultValue;
		private boolean hasValues = false;

		public DefaultIfEmptyAction(Subscriber<? super T> actual, T defaultValue) {
			super(actual);
			this.defaultValue = defaultValue;
		}

		@Override
		protected void doNext(T ev) {
			hasValues = true;
			subscriber.onNext(ev);
		}

		@Override
		protected void doComplete() {
			if (!hasValues) {
				subscriber.onNext(defaultValue);
			}
			subscriber.onComplete();
		}

	}

}
