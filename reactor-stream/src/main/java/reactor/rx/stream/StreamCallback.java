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
public final  class StreamCallback<T> extends StreamBarrier<T, T> {

	private final Consumer<? super T> consumer;
	private final Runnable completeConsumer;

	public StreamCallback(Publisher<T> source, Consumer<? super T> consumer, Runnable completeConsumer) {
		super(source);
		this.consumer = consumer;
		this.completeConsumer = completeConsumer;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new CallbackAction<>(subscriber, consumer, completeConsumer);
	}

	static final class CallbackAction<T> extends SubscriberBarrier<T, T> {

		private final Consumer<? super T> consumer;
		private final Runnable completeConsumer;

		public CallbackAction(Subscriber<? super T> actual, Consumer<? super T> consumer, Runnable completeConsumer) {
			super(actual);
			this.consumer = consumer;
			this.completeConsumer = completeConsumer;
		}

		@Override
		protected void doNext(T ev) {
			if(consumer != null){
				consumer.accept(ev);
			}
			subscriber.onNext(ev);
		}

		@Override
		protected void doComplete() {
			if(completeConsumer != null){
				completeConsumer.run();
			}
			subscriber.onComplete();
		}
	}
}
