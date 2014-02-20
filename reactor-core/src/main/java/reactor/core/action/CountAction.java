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
package reactor.core.action;

import reactor.core.Observable;
import reactor.event.Event;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class CountAction<T> extends Action<T> implements Flushable<T>{

	private final AtomicLong counter = new AtomicLong(0l);

	public CountAction(Observable d, Object successKey, Object failureKey) {
		super(d, successKey, failureKey);
	}

	@Override
	public void doAccept(Event<T> value) {
		counter.getAndIncrement();
	}


	@Override
	public CountAction<T> flush() {
		notifyValue(Event.wrap(counter.get()));
		counter.set(0l);
		return this;
	}
}
