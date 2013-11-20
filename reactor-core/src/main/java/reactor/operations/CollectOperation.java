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
package reactor.operations;

import reactor.core.Observable;
import reactor.event.Event;
import reactor.event.selector.Selector;
import reactor.function.Consumer;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Stephane Maldini
 */
public class CollectOperation<T> extends BaseOperation<T> {
	private final List<T> values = new ArrayList<T>();

	public CollectOperation(Observable d, Object successKey, Object failureKey) {
		super(d, successKey, failureKey);
	}

	@Override
	public void doOperation(Event<T> value) {
		synchronized (values) {
			values.add(value.getData());
		}
	}

	public CollectOperation<T> attachFlush(Selector selector) {
		getObservable().on(selector, new Consumer<Event<Void>>() {
			@Override
			public void accept(Event<Void> ev) {
				synchronized (values) {
					if (values.isEmpty()) {
						return;
					}
					notifyValue(ev.copy(new ArrayList<T>(values)));
					values.clear();
				}
			}
		});

		return this;
	}
}
