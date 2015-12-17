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

import org.reactivestreams.Subscriber;
import reactor.Publishers;
import reactor.core.error.CancelException;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Consumer;

/**
 * @author Stephane Maldini
 * @since 1.1, 2.0, 2.1
 */
final public class ErrorOperator<T, E extends Throwable> implements Publishers.Operator<T, T> {

	private final Consumer<? super E> consumer;
	private final Class<E>            selector;

	public ErrorOperator(Class<E> selector, Consumer<? super E> consumer) {
		this.consumer = consumer;
		this.selector = selector;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new ErrorAction<>(subscriber, selector, consumer);
	}

	final static class ErrorAction<T, E extends Throwable> extends SubscriberBarrier<T, T> {

		private final Consumer<? super E> consumer;
		private final Class<E>            selector;

		public ErrorAction(Subscriber<? super T> actual,
				Class<E> selector, Consumer<? super E> consumer) {
			super(actual);
			this.consumer = consumer;
			this.selector = selector;
		}

		@Override
		protected void doNext(T ev) {
			try {
				subscriber.onNext(ev);
			}
			catch (CancelException c){
				doError(c);
				throw c;
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		protected void doError(Throwable cause) {
			if (selector.isAssignableFrom(cause.getClass())) {
				if (consumer != null) {
					consumer.accept((E) cause);
				}
			}
			super.doError(cause);
		}

		@Override
		public String toString() {
			return super.toString() + "{" +
					"catch-type=" + selector +
					'}';
		}
	}

}
