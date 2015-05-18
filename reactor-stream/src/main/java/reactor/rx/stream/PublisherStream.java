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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.support.Exceptions;
import reactor.rx.Stream;
import reactor.rx.subscription.ReactiveSubscription;

/**
 * A {@link org.reactivestreams.Publisher} wrapper that takes care of lazy subscribing.
 * <p>
 * The stream will directly forward all the signals passed to the subscribers and complete when onComplete is called.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * <pre>
 * {@code
 * Streams.create(sub -> sub.onNext(1))
 * }
 * </pre>
 *
 * @author Stephane Maldini
 */
public class PublisherStream<T> extends Stream<T> {

	private final Publisher<T> source;

	public PublisherStream(Publisher<T> publisher) {
		this.source = publisher;
	}

	@Override
	public void subscribe(final Subscriber<? super T> subscriber) {
		try {
			subscriber.onSubscribe(new ReactiveSubscription<T>(PublisherStream.this, subscriber) {

				private boolean started = false;
				private ReactiveSubscription<T> thiz = this;
				private Subscription subscription;

				@Override
				protected void onRequest(long elements) {
					super.onRequest(elements);

					if (!started) {
						started = true;
						source.subscribe(new Subscriber<T>() {
							@Override
							public void onSubscribe(Subscription s) {
								subscription = s;
							}

							@Override
							public void onNext(T t) {
								thiz.onNext(t);
							}

							@Override
							public void onError(Throwable t) {
								thiz.onError(t);
							}

							@Override
							public void onComplete() {
								thiz.onComplete();
							}
						});
					} else if (subscription != null) {
						subscription.request(elements);
					}
				}

				@Override
				public void cancel() {
					super.cancel();
					if (subscription != null) {
						subscription.cancel();
					}
				}
			});
		} catch (Throwable throwable) {
			Exceptions.throwIfFatal(throwable);
			subscriber.onError(throwable);
		}
	}
}
