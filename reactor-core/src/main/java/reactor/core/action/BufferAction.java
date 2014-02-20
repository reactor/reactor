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
import reactor.function.Consumer;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class BufferAction<T> extends BatchAction<T> implements Flushable<T> {

	private final List<Event<T>> values;

	public static final Event<Object> BUFFER_FLUSH = Event.wrap(null);

	final private Consumer<Iterable<Event<T>>> batchConsumer;

	public BufferAction(int batchSize, final Observable d, Object successKey, Object failureKey, final Object flushKey) {
		super(batchSize, d, successKey, failureKey);
		if (flushKey == null) {
			this.batchConsumer = d.batchNotify(successKey);
		} else {
			this.batchConsumer = d.batchNotify(successKey, new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					d.notify(flushKey, BUFFER_FLUSH);
				}
			});
		}
		this.values = new ArrayList<Event<T>>(batchSize > 1 ? batchSize : 256);
	}

	@Override
	public void doNext(Event<T> value) {
		values.add(value);
	}


	@Override
	public void doFlush(Event<T> value) {
		if (values.isEmpty()) {
			return;
		}
		batchConsumer.accept(new ArrayList<Event<T>>(values));
		values.clear();
	}

	@Override
	public Flushable<T> flush() {
		doFlush(null);
		return this;
	}
}
