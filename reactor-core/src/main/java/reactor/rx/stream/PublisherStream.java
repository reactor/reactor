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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.rx.Stream;
import reactor.rx.action.Action;

import javax.annotation.Nonnull;

/**
 * A {@link org.reactivestreams.Publisher} wrapper that takes care of lazy subscribing.
 *
 * The stream will directly forward all the signals passed to the subscribers and complete when onComplete is called.
 *
 * Create such stream with the provided factory, E.g.:
 * {@code
 * Streams.create(sub -> sub.onNext(1))
 * }
 *
 * @author Stephane Maldini
 */
public class PublisherStream<T> extends Stream<T> {

	private final Publisher<? extends T> publisher;

	public PublisherStream(@Nonnull Dispatcher dispatcher, Publisher<? extends T> publisher) {
		super(dispatcher);
		this.publisher = publisher;
	}

	@Override
	@SuppressWarnings("unchecked")
	public PublisherStream<T> env(Environment environment) {
		return (PublisherStream<T>) super.env(environment);
	}

	@Override
	@SuppressWarnings("unchecked")
	public PublisherStream<T> capacity(long elements) {
		return (PublisherStream<T>) super.capacity(elements);
	}

	/**
	 * Provide a unique staging action in between the publisher and future actions to subscribe to it.
	 *
	 * @return a new a action
	 */
	public Action<T, T> defer() {
		return new DeferredSubscribeAction();
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		publisher.subscribe(subscriber);
	}

	private class DeferredSubscribeAction extends Action<T, T> {
		@Override
		protected void doNext(T ev) {
			broadcastNext(ev);
		}

		@Override
		public void subscribe(final Subscriber<? super T> subscriber) {
			dispatch(new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					DeferredSubscribeAction.super.subscribe(subscriber);
					publisher.subscribe(DeferredSubscribeAction.this);
				}
			});
		}
	}
}
