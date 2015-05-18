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
import reactor.core.support.Exceptions;
import reactor.rx.Stream;
import reactor.rx.subscription.PushSubscription;

/**
 * A Stream that emits only one value and then complete.
 * <p>
 * Since the stream retains the value in a final field, any {@link this#subscribe(org.reactivestreams.Subscriber)}
 * will replay the value. This is a "Cold" stream.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * <pre>
 * {@code
 * Streams.just(1).consume(
 *    log::info,
 *    log::error,
 *    (-> log.info("complete"))
 * )
 * }
 * </pre>
 * Will log:
 * <pre>
 * {@code
 * 1
 * complete
 * }
 * </pre>
 *
 * @author Stephane Maldini
 */
public final class SingleValueStream<T> extends Stream<T> {

	final public static SingleValueStream<?> EMPTY = new SingleValueStream<>(null);

	final private T value;

	@SuppressWarnings("unchecked")
	public SingleValueStream(T value) {
		this.value = value;
	}

	@Override
	public void subscribe(final Subscriber<? super T> subscriber) {
		try {
			subscriber.onSubscribe(new PushSubscription<T>(this, subscriber) {
				boolean terminado = false;

				@Override
				public void request(long elements) {
					if (terminado) return;

					terminado = true;
					if (value != null) {
						onNext(value);
					}
					onComplete();
				}
			});
		} catch (Throwable throwable) {
			Exceptions.throwIfFatal(throwable);
			subscriber.onError(throwable);
		}
	}

	@Override
	public String toString() {
		return "singleValue=" + value;
	}
}
