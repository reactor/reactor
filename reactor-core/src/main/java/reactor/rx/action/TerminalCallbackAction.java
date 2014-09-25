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
public class TerminalCallbackAction<T> extends Action<T, Void> {

	private final Consumer<? super T>         consumer;
	private final Consumer<? super Throwable> errorConsumer;
	private final Consumer<Void>              completeConsumer;

	public TerminalCallbackAction(Dispatcher dispatcher, Consumer<? super T> consumer,
	                              Consumer<? super Throwable> errorConsumer, Consumer<Void> completeConsumer) {
		super(dispatcher);
		this.consumer = consumer;
		this.errorConsumer = errorConsumer;
		this.completeConsumer = completeConsumer;
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		capacity = firehose ? Long.MAX_VALUE : capacity;
		requestConsumer.accept(capacity);
	}

	@Override
	protected void doNext(T ev) {
		consumer.accept(ev);
		if (pendingNextSignals == 0 && currentNextSignals >= capacity) {
			requestConsumer.accept(currentNextSignals);
		}
	}

	@Override
	protected void doError(Throwable ev) {
		if(errorConsumer != null){
			errorConsumer.accept(ev);
		}
		super.doError(ev);
	}

	@Override
	protected void doComplete() {
		if(completeConsumer != null){
			completeConsumer.accept(null);
		}
		super.doComplete();
	}

	@Override
	protected void doPendingRequest() {
	}
}
