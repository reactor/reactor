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
import reactor.bus.Observable;
import reactor.bus.registry.Registration;
import reactor.bus.selector.Selector;
import reactor.fn.Consumer;
import reactor.rx.Stream;
import reactor.rx.subscription.PushSubscription;

import javax.annotation.Nonnull;

/**
 * Emit signals whenever an Event arrives from the {@link reactor.bus.selector.Selector} topic from the {@link
 * reactor.bus.Observable}.
 * This stream will never emit a {@link org.reactivestreams.Subscriber#onComplete()}.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * <pre>
 * {@code
 * Streams.on(reactor, $("topic")).consume(System.out::println)
 * }
 * </pre>
 *
 * @author Stephane Maldini
 */
public final class ObservableStream<T> extends Stream<T> {

	private final Selector      selector;
	private final Observable<T> observable;


	public ObservableStream(final @Nonnull Observable<T> observable,
	                        final @Nonnull Selector selector) {

		this.selector = selector;
		this.observable = observable;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		s.onSubscribe(new PushSubscription<T>(this, s) {

			final Registration<Consumer<? extends T>> registration = observable.on(selector, new Consumer<T>() {
				@Override
				public void accept(T event) {
					subscriber.onNext(event);
				}
			});

			@Override
			public void cancel() {
				super.cancel();
				registration.cancel();
			}
		});
	}

	@Override
	public String toString() {
		return "ObservableStream{" +
				"selector=" + selector +
				", observable=" + observable +
				'}';
	}
}
