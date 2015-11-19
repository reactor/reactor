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

import org.reactivestreams.Subscriber;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Function;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
final public class ErrorReturnOperator<T, E extends Throwable> implements Publishers.Operator<T, T> {

	private final Function<? super E, ? extends T> function;
	private final Class<E>                         selector;

	public ErrorReturnOperator(Class<E> selector, Function<? super E, ? extends T> function) {
		this.function = function;
		this.selector = selector;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new ErrorReturnAction<>(subscriber, selector, function);
	}

	final static class ErrorReturnAction<T, E extends Throwable> extends SubscriberBarrier<T, T> {

		private final Function<? super E, ? extends T> function;
		private final Class<E>                         selector;

		public ErrorReturnAction(Subscriber<? super T> actual,
				Class<E> selector,
				Function<? super E, ? extends T> function) {
			super(actual);
			this.function = function;
			this.selector = selector;
		}

		@Override
		@SuppressWarnings("unchecked")
		protected void doError(Throwable cause) {
			if (selector.isAssignableFrom(cause.getClass())) {
				subscriber.onNext(function.apply((E) cause));
				subscriber.onComplete();
			}
			else {
				subscriber.onError(cause);
			}
		}

		@Override
		public String toString() {
			return super.toString() + "{" +
					"catch-type=" + selector +
					'}';
		}
	}

}
