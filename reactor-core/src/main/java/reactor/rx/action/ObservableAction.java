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

import org.reactivestreams.spi.Subscription;
import reactor.core.Observable;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class ObservableAction<T> extends Action<T, Void> {

	private final Observable observable;
	private final Object     key;
	private final AtomicInteger count = new AtomicInteger();

	public ObservableAction(Dispatcher dispatcher, Observable observable, Object key) {
		super(dispatcher);
		this.observable = observable;
		this.key = key;
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		available();
	}

	@Override
	protected void doNext(T ev) {
		int counted = count.getAndIncrement();
		observable.notify(key, Event.wrap(ev));
		if(counted >= batchSize){
			available();
		}
	}

}
