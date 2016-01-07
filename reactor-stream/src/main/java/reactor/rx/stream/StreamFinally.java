/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Consumer;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.5
 */
public final  class StreamFinally<T> extends StreamBarrier<T, T> {

	private final Consumer<Signal<T>> consumer;

	public StreamFinally(Publisher<T> source, Consumer<Signal<T>> consumer) {
		super(source);
		this.consumer = consumer;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new FinallyAction<>(subscriber, consumer);
	}

	static final class FinallyAction<T> extends SubscriberBarrier<T, T> {

		private final Consumer<Signal<T>> consumer;

		public FinallyAction(Subscriber<? super T> actual, Consumer<Signal<T>> consumer) {
			super(actual);
			this.consumer = consumer;
		}

		@Override
		protected void doError(Throwable ev) {
			subscriber.onError(ev);
			consumer.accept(Signal.<T>error(ev));
		}

		@Override
		protected void doComplete() {
			subscriber.onComplete();
			consumer.accept(Signal.<T>complete());
		}

	}

}
