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
import reactor.rx.Stream;
import reactor.rx.subscription.PushSubscription;

import java.util.Iterator;

/**
 * A Stream that emits {@link java.lang.Iterable} values one by one and then complete.
 * <p>
 * Since the stream retains the iterable in a final field, any {@link this#subscribe(org.reactivestreams.Subscriber)}
 * will replay all the iterable. This is a "Cold" stream.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * {@code
 * Streams.just(1,2,3,4).consume(
 *    log::info,
 *    log::error,
 *    (-> log.info("complete"))
 * )
 * }
 * <p>
 * Will log:
 * 1
 * 2
 * 3
 * 4
 * complete
 *
 * @author Stephane Maldini
 */
public final class IterableStream<T> extends Stream<T> {

	final private Iterable<? extends T> defaultValues;

	public IterableStream(Iterable<? extends T> defaultValues) {
		this.defaultValues = defaultValues;
	}

	@Override
	public void subscribe(final Subscriber<? super T> subscriber) {
		if (defaultValues != null) {
			subscriber.onSubscribe(new PushSubscription<T>(this, subscriber) {
				final Iterator<? extends T> iterator = defaultValues.iterator();

				@Override
				public void request(long elements) {
					long i = 0;
					while (i < elements && iterator.hasNext()) {
						onNext(iterator.next());
						i++;
					}

					if (!iterator.hasNext()) {
						onComplete();
					}
				}
			});
		} else {
			subscriber.onComplete();
		}
	}

	@Override
	public String toString() {
		return "iterable=" + defaultValues;
	}
}
