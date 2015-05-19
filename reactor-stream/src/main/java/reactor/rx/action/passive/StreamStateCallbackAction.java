/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.rx.action.passive;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.fn.Consumer;
import reactor.rx.action.Action;

/**
 * @author Stephane Maldini
 */
public class StreamStateCallbackAction<T> extends Action<T, T> {

	private final Consumer<? super Subscriber<? super T>> subscribeConsumer;
	private final Consumer<Void> cancelConsumer;
	private final Consumer<? super Subscription> onSubscribeConsumer;

	public StreamStateCallbackAction(Consumer<? super Subscriber<? super T>> subscribeConsumer,
	                                 Consumer<Void> cancelConsumer,
	                                 Consumer<? super Subscription> onSubscribeConsumer) {
		this.subscribeConsumer = subscribeConsumer;
		this.cancelConsumer = cancelConsumer;
		this.onSubscribeConsumer = onSubscribeConsumer;
	}

	@Override
	protected void doOnSubscribe(Subscription subscription) {
		if(onSubscribeConsumer != null){
			onSubscribeConsumer.accept(subscription);
		}
		super.doOnSubscribe(subscription);
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
	}

	@Override
	protected void doShutdown() {
		if(cancelConsumer != null){
			cancelConsumer.accept(null);
		}
		super.doShutdown();
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		if(subscribeConsumer != null){
			subscribeConsumer.accept(subscriber);
		}
		super.subscribe(subscriber);
	}
}
