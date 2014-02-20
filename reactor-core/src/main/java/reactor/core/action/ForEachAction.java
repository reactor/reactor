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
import reactor.event.selector.Selector;
import reactor.function.Consumer;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class ForEachAction<T> extends Action<Iterable<T>> implements Flushable<T> {

	public static final Event<Object> FOREACH_FLUSH = Event.wrap(null);

	final private Consumer<Iterable<Event<T>>> batchConsumer;
	final private int                          batchSize;
	final private Iterable<T>                  defaultValues;

	public ForEachAction(int batchSize, Observable d, Object successKey, Object failureKey, Object flushKey) {
		this(null, batchSize, d, successKey, failureKey, flushKey);
	}


	public ForEachAction(Iterable<T> defaultValues, int batchSize, final Observable d, Object successKey,
	                     Object failureKey, final Object flushKey) {
		super(d, successKey, failureKey);
		this.defaultValues = defaultValues;
		this.batchConsumer = d.batchNotify(successKey, new Consumer<Void>() {
			@Override
			public void accept(Void aVoid) {
				d.notify(flushKey, FOREACH_FLUSH);
			}
		});
		this.batchSize = batchSize > 0 ? batchSize : 256;
	}

	@Override
	public void doAccept(Event<Iterable<T>> value) {
		if (value.getData() != null) {
			List<Event<T>> evs = new ArrayList<Event<T>>(batchSize);
			for (T data : value.getData()) {
				evs.add(value.copy(data));
			}
			batchConsumer.accept(evs);
		}
	}

	@Override
	public Flushable<T> flush() {
		doAccept(Event.wrap(defaultValues));
		return this;
	}
}
