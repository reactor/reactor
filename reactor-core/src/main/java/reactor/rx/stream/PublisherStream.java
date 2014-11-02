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
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.function.Consumer;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.subscription.ReactiveSubscription;

/**
 * A {@link org.reactivestreams.Publisher} wrapper that takes care of lazy subscribing.
 * <p>
 * The stream will directly forward all the signals passed to the subscribers and complete when onComplete is called.
 * <p>
 * Create such stream with the provided factory, E.g.:
 * {@code
 * Streams.create(sub -> sub.onNext(1))
 * }
 *
 * @author Stephane Maldini
 */
public class PublisherStream<T> extends Stream<T> {

	private final Publisher<T> publisher;

	public PublisherStream(Publisher<T> publisher) {
		this.publisher = publisher;
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

		public DeferredSubscribeAction() {
			super(SynchronousDispatcher.INSTANCE);
		}

		@Override
		protected void doNext(T ev) {
			broadcastNext(ev);
		}

		/*@Override
		protected PushSubscription<T> createSubscription(Subscriber<? super T> subscriber, boolean reactivePull) {
			return new PushSubscription<>(this, subscriber);
		}*/

		@Override
		protected void doError(Throwable ev) {
			super.doError(ev);
		}

		@Override
		public void subscribe(final Subscriber<? super T> subscriber) {
			final Publisher<T> deferredPublisher = publisher;

			dispatch(new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					subscribeWithSubscription(subscriber, new ReactiveSubscription<T>(DeferredSubscribeAction.this, subscriber) {

						private boolean started = false;

						@Override
						protected void onRequest(long elements) {
							super.onRequest(elements);

							if(!started){
								started = true;
								deferredPublisher.subscribe(DeferredSubscribeAction.this);
								DeferredSubscribeAction.this.onRequest(elements);
							}else{
								requestUpstream(capacity, buffer.isComplete(), elements);
							}

						}
					}, false);
				}
			});
		}
	}
}
