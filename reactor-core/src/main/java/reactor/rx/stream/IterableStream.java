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
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;

import java.util.Collection;
import java.util.Iterator;

/**
 * A Stream that emits {@link java.lang.Iterable} values one by one and then complete.
 * <p>
 * Since the stream retains the iterable in a final field, any {@link this#subscribe(org.reactivestreams.Subscriber)}
 * will replay all the iterable. This is a "Cold" stream.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * {@code
 * Streams.defer(1,2,3,4).consume(
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
public class IterableStream<T> extends Stream<T> {

	final private Iterable<? extends T> defaultValues;

	@SuppressWarnings("unchecked")
	public IterableStream(Iterable<? extends T> defaultValues,
	                      Dispatcher dispatcher) {
		super(dispatcher);

		this.defaultValues = defaultValues;
		state = State.COMPLETE;
		if (null != defaultValues && Collection.class.isAssignableFrom(defaultValues.getClass())) {
			capacity(((Collection<T>) defaultValues).size());
		}
		keepAlive(true);
	}

	@Override
	public void checkAndSubscribe(final Subscriber<? super T> subscriber, final StreamSubscription<T> streamSubscription) {
		if (defaultValues != null) {
			if (addSubscription(streamSubscription)) {
				if (streamSubscription.asyncManaged()) {
					subscriber.onSubscribe(streamSubscription);
				} else {
					dispatch(new Consumer<Void>() {
						@Override
						public void accept(Void aVoid) {
							subscriber.onSubscribe(streamSubscription);
						}
					});
				}
			}
		} else {
			subscriber.onComplete();
		}
	}

	@Override
	protected StreamSubscription<T> createSubscription(Subscriber<? super T> subscriber, boolean reactivePull) {
		if(defaultValues != null) {
			return new StreamSubscription<T>(this, subscriber) {
				Iterator<? extends T> iterator = defaultValues.iterator();

				@Override
				public void request(long elements) {
					super.request(elements);

					if (buffer.isComplete()) return;

					long i = 0;
					while (i < elements && iterator.hasNext()) {
						onNext(iterator.next());
						i++;
					}

					if (!iterator.hasNext() && !buffer.isComplete()) {
						onComplete();
					}
				}
			};
		}else{
			return null;
		}
	}

	@Override
	public String toString() {
		return super.toString() + " " + defaultValues;
	}
}
