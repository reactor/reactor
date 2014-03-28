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

import com.gs.collections.impl.block.function.checked.CheckedFunction;
import com.gs.collections.impl.utility.Iterate;
import reactor.core.Observable;
import reactor.event.Event;
import reactor.function.Consumer;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class ForEachAction<T> extends Action<Iterable<T>> implements Flushable<T> {

	public static final Event<Object> FOREACH_FLUSH = Event.wrap(null);

	final private Consumer<Iterable<Event<T>>> batchConsumer;
	final private Iterable<T>                  defaultValues;

	public ForEachAction(Observable d,
	                     Object successKey,
	                     Object failureKey,
	                     Object flushKey) {
		this(null, d, successKey, failureKey, flushKey);
	}


	public ForEachAction(Iterable<T> defaultValues,
	                     final Observable d,
	                     Object successKey,
	                     Object failureKey,
	                     final Object flushKey) {
		super(d, successKey, failureKey);
		this.defaultValues = defaultValues;
		this.batchConsumer = d.batchNotify(successKey, new Consumer<Void>() {
			@Override
			public void accept(Void aVoid) {
				d.notify(flushKey, FOREACH_FLUSH);
			}
		});
	}

	@Override
	public void doAccept(final Event<Iterable<T>> value) {
		Iterable<T> data;
		if (null == (data = value.getData())) {
			return;
		}
		batchConsumer.accept(Iterate.collect(data, new CheckedFunction<T, Event<T>>() {
			@Override
			public Event<T> safeValueOf(T data) throws Exception {
				return value.copy(data);
			}
		}));
	}

	@Override
	public Flushable<T> flush() {
		doAccept(Event.wrap(defaultValues));
		return this;
	}
}
