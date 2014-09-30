/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

import org.reactivestreams.Subscriber;
import reactor.rx.Stream;
import reactor.rx.subscription.PushSubscription;

/**
 * A Stream that emits only one value and then complete.
 * <p>
 * Since the stream retains the value in a final field, any {@link this#subscribe(org.reactivestreams.Subscriber)}
 * will replay the value. This is a "Cold" stream.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * {@code
 * Streams.just(1).consume(
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
public final class SingleValueStream<T> extends Stream<T> {

	final private T value;

	@SuppressWarnings("unchecked")
	public SingleValueStream(T value) {
		this.value = value;
	}

	@Override
	public void subscribe(final Subscriber<? super T> subscriber) {
		if (value != null) {
			subscriber.onSubscribe(new PushSubscription<T>(this, subscriber) {
				boolean terminado = false;
				@Override
				public void request(long elements) {
					if(terminado)return;

					terminado = true;
					onNext(value);
					onComplete();
				}
			});
		} else {
			subscriber.onComplete();
		}
	}

	@Override
	public String toString() {
		return "singleValue=" + value;
	}
}
