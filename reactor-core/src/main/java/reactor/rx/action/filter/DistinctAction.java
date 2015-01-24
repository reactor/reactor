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
import reactor.fn.Function;
import reactor.rx.action.Action;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Anatoly Kadyshev
 * @since 2.0
 */
public class DistinctAction<T, V> extends Action<T, T> {

	private final Set<V> keySet = new HashSet<V>();

	private final Function<? super T, ? extends V> keySelector;

	public DistinctAction(Function<? super T, ? extends V> keySelector, Dispatcher dispatcher) {
		super(dispatcher);
		this.keySelector = keySelector;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doNext(T currentData) {
		V currentKey;
		if (keySelector != null) {
			currentKey = keySelector.apply(currentData);
		} else {
			currentKey = (V) currentData;
		}

		if(keySet.add(currentKey)) {
			broadcastNext(currentData);
		}
	}

	@Override
	protected void doComplete() {
		super.doComplete();
		keySet.clear();
	}

}
