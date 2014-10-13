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
import reactor.timer.Timer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class BufferAction<T> extends BatchAction<T, List<T>> {

	private final List<T> values = new ArrayList<T>();

	public BufferAction(Dispatcher dispatcher, int batchsize) {
		super(dispatcher, batchsize, true, false, true);
	}

	public BufferAction(Dispatcher dispatcher, int maxSize, long timespan, TimeUnit unit, Timer timer) {
		super(dispatcher, maxSize, true, false, true, timespan, unit, timer);
	}

	@Override
	public void nextCallback(T value) {
		values.add(value);
	}

	@Override
	public void flushCallback(T ev) {
		if (values.isEmpty()) {
			return;
		}
		broadcastNext(new ArrayList<T>(values));
		values.clear();
	}

}
