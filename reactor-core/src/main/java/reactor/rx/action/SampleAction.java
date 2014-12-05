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
import reactor.timer.Timer;

import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class SampleAction<T> extends BatchAction<T, T> {

	private T sample;

	public SampleAction(Dispatcher dispatcher, int maxSize) {
		this(dispatcher, maxSize, false);
	}

	public SampleAction(Dispatcher dispatcher, boolean first, int maxSize, long timespan, TimeUnit unit, Timer timer) {
		super(dispatcher, maxSize, !first, first, true, timespan, unit, timer);
	}


	public SampleAction(Dispatcher dispatcher, int maxSize, boolean first) {
		super(dispatcher, maxSize, !first, first, true);
	}

	@Override
	protected void firstCallback(T event) {
		sample = event;
	}


	@Override
	protected void nextCallback(T event) {
		sample = event;
	}

	@Override
	protected void flushCallback(T event) {
		if(sample != null){
			T _last = sample;
			sample = null;
			broadcastNext(_last);
		}
	}

	@Override
	public String toString() {
		return super.toString()+" sample="+sample;
	}
}
