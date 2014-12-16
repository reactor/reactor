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
package reactor.rx.action;

import reactor.core.Dispatcher;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class TimestampAction<T> extends Action<T, Tuple2<Long, T>> {

	public TimestampAction(Dispatcher dispatcher) {
		super(dispatcher);
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(Tuple.of(System.currentTimeMillis(), ev));
	}
}
