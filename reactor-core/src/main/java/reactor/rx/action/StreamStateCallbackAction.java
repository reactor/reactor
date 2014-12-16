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
package reactor.rx.action;

import org.reactivestreams.Subscriber;
import reactor.core.Dispatcher;
import reactor.fn.Consumer;

/**
 * @author Stephane Maldini
 */
public class StreamStateCallbackAction<T> extends Action<T, T> {

	private final Consumer<? super Subscriber<? super T>> subscribeConsumer;
	private final Consumer<Void> cancelConsumer;

	public StreamStateCallbackAction(Dispatcher dispatcher, Consumer<? super Subscriber<? super T>> subscribeConsumer,
	                                 Consumer<Void> cancelConsumer) {
		super(dispatcher);
		this.subscribeConsumer = subscribeConsumer;
		this.cancelConsumer = cancelConsumer;
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
	}

	@Override
	protected void onShutdown() {
		super.onShutdown();
		if(cancelConsumer != null){
			trySyncDispatch(null, cancelConsumer);
		}
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		super.subscribe(subscriber);
		if(subscribeConsumer != null){
			trySyncDispatch(subscriber, subscribeConsumer);
		}
	}
}
