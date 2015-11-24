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
package reactor.rx.action.combination;

import java.util.Iterator;

import org.reactivestreams.Subscriber;
import reactor.Publishers;
import reactor.Subscribers;
import reactor.core.error.Exceptions;
import reactor.core.subscriber.SubscriberWithDemand;
import reactor.fn.BiFunction;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.1
 */
public final class ZipWithIterableOperator<I, IT, V> implements Publishers.Operator<I, V> {

	final BiFunction<? super I, ? super IT, ? extends V> combinator;
	final Iterable<? extends IT> iterable;

	public ZipWithIterableOperator(BiFunction<? super I, ? super IT, ? extends V> combinator,
			Iterable<? extends IT> iterable) {
		this.combinator = combinator;
		this.iterable = iterable;
	}

	@Override
	public Subscriber<? super I> apply(Subscriber<? super V> subscriber) {
		Iterator<? extends IT> iterator = iterable.iterator();
		try {
			if (!iterator.hasNext()) {
				subscriber.onComplete();
				return Subscribers.empty();
			}
		} catch (Throwable e) {
			Exceptions.throwIfFatal(e);
			subscriber.onError(e);
		}

		return new ZipWithIterableAction<>(subscriber, combinator, iterator);
	}

	final static class ZipWithIterableAction<I, IT, V> extends SubscriberWithDemand<I, V> {

		final BiFunction<? super I, ? super IT, ? extends V> combinator;
		final Iterator<? extends IT>             iterator;

		public ZipWithIterableAction(Subscriber<? super V> subscriber,
				BiFunction<? super I, ? super IT, ? extends V> combinator,
				Iterator<? extends IT> iterator) {
			super(subscriber);
			this.combinator = combinator;
			this.iterator = iterator;
		}

		@Override
		protected void doNext(I t) {
			subscriber.onNext(combinator.apply(t, iterator.next()));
			if (!iterator.hasNext() && TERMINATED.compareAndSet(this, NOT_TERMINATED, TERMINATED_WITH_SUCCESS)) {
				subscriber.onComplete();
			}
		}
	}
}
