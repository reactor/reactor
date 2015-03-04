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
import reactor.bus.Bus;
import reactor.bus.EventBus;
import reactor.bus.registry.Registration;
import reactor.bus.selector.Selector;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.fn.Consumer;
import reactor.rx.Stream;
import reactor.rx.action.support.SerializedSubscriber;
import reactor.rx.subscription.PushSubscription;

import javax.annotation.Nonnull;

/**
 * Emit signals whenever an Event arrives from the {@link reactor.bus.selector.Selector} topic from the {@link
 * reactor.bus.Bus}.
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
public final class BusStream<T> extends Stream<T> {

	private final Selector      selector;
	private final Bus<T> observable;
	private final Dispatcher    dispatcher;
	private final boolean    ordering;


	public BusStream(final @Nonnull Bus<T> observable,
	                 final @Nonnull Selector selector) {

		this.selector = selector;
		this.observable = observable;
		this.dispatcher = EventBus.class.isAssignableFrom(observable.getClass()) ?
				((EventBus)observable).getDispatcher() : SynchronousDispatcher.INSTANCE;
		this.ordering = dispatcher.supportsOrdering();
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		if(!ordering) {
			s = SerializedSubscriber.create(s);
		}
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
	public final Dispatcher getDispatcher() {
		return ordering ? dispatcher : SynchronousDispatcher.INSTANCE;
	}

	@Override
	public String toString() {
		return "BusStream{" +
				"selector=" + selector +
				", observable=" + observable +
				'}';
	}
}
