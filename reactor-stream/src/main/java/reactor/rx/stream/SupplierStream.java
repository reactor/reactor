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
import reactor.core.error.Exceptions;
import reactor.fn.Supplier;
import reactor.rx.Stream;
import reactor.rx.subscription.PushSubscription;

/**
 * A Stream that returns the result from {@link reactor.fn.Supplier#get()} everytime it is requested via
 * {@link org.reactivestreams.Subscription#request(long)}.
 * <p>
 * The Stream will end when the result is null or {@link reactor.rx.action.Action#broadcastComplete()} is called.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * <pre>
 * {@code
 * Streams.generate(env, (-> randomNumber()))
 *    .throttle(200)
 *    .consume(System.out::println)
 * }
 * </pre>
 * This example will generate a random number every 200ms
 *
 * @author Stephane Maldini
 */
public final class SupplierStream<T> extends Stream<T> {

	private final Supplier<? extends T> supplier;

	public SupplierStream(Supplier<? extends T> supplier) {
		this.supplier = supplier;
	}

	@Override
	public void subscribe(final Subscriber<? super T> subscriber) {
		try {
			if (supplier != null) {
				subscriber.onSubscribe(new PushSubscription<T>(this, subscriber) {

					@Override
					public void request(long elements) {
						try {
							supplyValue(subscriber);
						} catch (Throwable throwable) {
							Exceptions.throwIfFatal(throwable);
							subscriber.onError(throwable);
						}
					}
				});

			} else {
				subscriber.onComplete();
			}
		} catch (Throwable throwable) {
			Exceptions.throwIfFatal(throwable);
			subscriber.onError(throwable);
		}
	}

	private void supplyValue(final Subscriber<? super T> subscriber) {
		T supplied = supplier.get();
		if (supplied != null) {
			subscriber.onNext(supplied);
		} else {
			subscriber.onComplete();
		}
	}

}
