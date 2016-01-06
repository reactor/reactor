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
import org.reactivestreams.Subscription;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.fn.Consumer;

/**
 * @author Stephane Maldini
 * @since 2.0, 2.5
 */
public class StreamStateCallback<T> extends StreamBarrier<T, T> {

	private final Runnable                          cancelConsumer;
	private final Consumer<? super Subscription>          onSubscribeConsumer;

	public StreamStateCallback(Publisher<T> source, Runnable cancelConsumer,
			Consumer<? super Subscription> onSubscribeConsumer) {
		super(source);
		this.cancelConsumer = cancelConsumer;
		this.onSubscribeConsumer = onSubscribeConsumer;
	}

	@Override
	public Subscriber<? super T> apply(Subscriber<? super T> subscriber) {
		return new StreamStateCallbackAction<>(subscriber, cancelConsumer, onSubscribeConsumer);
	}

	static final class StreamStateCallbackAction<T> extends SubscriberBarrier<T, T> {

		private final Runnable                    cancelConsumer;
		private final Consumer<? super Subscription>          onSubscribeConsumer;

		public StreamStateCallbackAction(Subscriber<? super T> actual,
				Runnable cancelConsumer,
				Consumer<? super Subscription> onSubscribeConsumer) {
			super(actual);
			this.cancelConsumer = cancelConsumer;
			this.onSubscribeConsumer = onSubscribeConsumer;
		}

		@Override
		protected void doOnSubscribe(Subscription subscription) {
			if (onSubscribeConsumer != null) {
				onSubscribeConsumer.accept(subscription);
			}
			subscriber.onSubscribe(this);
		}

		@Override
		protected void doCancel() {
			if (cancelConsumer != null) {
				cancelConsumer.run();
			}
			super.doCancel();
		}
	}
}
