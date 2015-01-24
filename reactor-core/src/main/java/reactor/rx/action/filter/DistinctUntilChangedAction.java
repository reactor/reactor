/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package reactor.rx.action.filter;

import reactor.core.Dispatcher;
import reactor.rx.action.Action;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class DistinctUntilChangedAction<T, V> extends Action<T, T> {

	private V lastKey;

	private final Function<? super T, ? extends V> keySelector;

	public DistinctUntilChangedAction(Function<? super T, ? extends V> keySelector, Dispatcher dispatcher) {
		super(dispatcher);
		this.keySelector = keySelector;
	}

	@Override
	protected void doNext(T currentData) {
		V currentKey;
		if (keySelector != null) {
			currentKey = keySelector.apply(currentData);
		} else {
			currentKey = (V) currentData;
		}

		if(currentKey == null || !currentKey.equals(lastKey)) {
			lastKey = currentKey;
			broadcastNext(currentData);
		}
	}
}
