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

import reactor.event.dispatch.Dispatcher;
import reactor.util.Assert;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class FirstAction<T> extends BatchAction<T, T> {

	public FirstAction(long batchSize, Dispatcher dispatcher) {
		super(batchSize, dispatcher, false, true, false);

		Assert.state(batchSize > 0, "Cannot first() an unbounded Stream. Try extracting a batch first.");
	}

	@Override
	protected void firstCallback(T event) {
		broadcastNext(event);
	}
}
