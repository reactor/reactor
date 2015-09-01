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
package reactor.rx.stream;

import org.reactivestreams.Subscriber;
import reactor.core.publisher.PublisherFactory;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.util.Iterator;

/**
 * A Stream that emits {@link java.lang.Iterable} values one by one and then complete.
 * <p>
 * Since the stream retains the iterable in a final field, any {@link org.reactivestreams.Subscriber}
 * will replay all the iterable. This is a "Cold" stream.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * <pre>
 * {@code
 * Streams.just(1,2,3,4).consume(
 *    log::info,
 *    log::error,
 *    (-> log.info("complete"))
 * )
 * }
 * </pre>
 * <pre>
 * //Will log:
 * 1
 * 2
 * 3
 * 4
 * complete
 * </pre>
 *
 * @author Stephane Maldini
 */
public final class IterableStream<T> {

	/**
	 * Create an Iterable Stream Publisher
	 *
	 * @param defaultValues
	 * @param <T>
	 * @return
	 */
	public static <T> Stream<T> create(final Iterable<? extends T> defaultValues) {
		return Streams.wrap(PublisherFactory.create(new Consumer<SubscriberWithContext<T, Iterator<? extends T>>>() {
			@Override
			public void accept(SubscriberWithContext<T, Iterator<? extends T>> subscriber) {
				final Iterator<? extends T> iterator = subscriber.context();
				if (iterator.hasNext()) {
					subscriber.onNext(iterator.next());
				} else {
					subscriber.onComplete();
					return;
				}

				if (!iterator.hasNext()) {
					subscriber.onComplete();
				}
			}

			@Override
			public String toString() {
				return "IterableStream="+defaultValues;
			}
		}, new Function<Subscriber<? super T>, Iterator<? extends T>>() {
			@Override
			public Iterator<? extends T> apply(Subscriber<? super T> subscriber) {
				if (defaultValues == null) {
					subscriber.onComplete();
					throw PublisherFactory.PrematureCompleteException.INSTANCE;
				}
				return defaultValues.iterator();
			}

			@Override
			public String toString() {
				return "cold="+defaultValues;
			}
		}));
	}
}
