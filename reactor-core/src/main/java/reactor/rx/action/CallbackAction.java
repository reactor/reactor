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
package reactor.rx.action;

import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;

/**
 * @author Stephane Maldini
 */
public class CallbackAction<T> extends Action<T, T> {

	private final Consumer<T> consumer;
	private final boolean     prefetch;

	public CallbackAction(Dispatcher dispatcher, Consumer<T> consumer, boolean prefetch) {
		super(dispatcher);
		this.consumer = consumer;
		this.prefetch = prefetch;
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		if (prefetch) {
			batchSize = firehose ? Integer.MAX_VALUE : batchSize;
			requestConsumer.accept(batchSize);
		} else {
			super.doSubscribe(subscription);
		}
	}

	@Override
	protected void doNext(T ev) {
		consumer.accept(ev);
		if(prefetch){
			if (currentNextSignals == batchSize) {
				requestConsumer.accept(batchSize);
			}
		}else{
			broadcastNext(ev);
		}
	}

	@Override
	public String toString() {
		return super.toString()+(!prefetch ? "{" +
				"observing"+
				'}' : "");
	}
}
