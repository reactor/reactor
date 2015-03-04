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
package reactor.rx.action.terminal;

import reactor.bus.Bus;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.fn.Function;
import reactor.rx.action.Action;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class BusAction<T> extends Action<T, Void> {

	private final Bus             observable;
	private final Function<? super T, ?> keyMapper;
	private final boolean                wrapEvent;
	private final Object                 key;

	public BusAction(Bus<?> observable, Object key, Function<? super T, ?> keyMapper) {
		this.observable = observable;
		this.wrapEvent = EventBus.class.isAssignableFrom(observable.getClass());
		this.key = key;
		this.keyMapper = keyMapper;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doNext(T ev) {
		observable.notify(keyMapper != null ? keyMapper.apply(ev) : key, wrapEvent ? Event.wrap(ev) : ev);
	}
}
