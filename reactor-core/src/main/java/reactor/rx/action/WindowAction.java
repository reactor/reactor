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
import reactor.rx.Stream;
import reactor.timer.Timer;

import java.util.concurrent.TimeUnit;

/**
 * WindowAction is forwarding events on a steam until {@param backlog} is reached,
 * after that streams collected events further, complete it and create a fresh new stream.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public class WindowAction<T> extends BatchAction<T, Stream<T>> {

	private Action<T, T> currentWindow;

	public WindowAction(Dispatcher dispatcher, int backlog) {
		super(dispatcher, backlog, true, true, true);
	}

	public WindowAction(Dispatcher dispatcher, int backlog, long timespan, TimeUnit unit, Timer timer) {
		super(dispatcher, backlog, true, true, true, timespan, unit, timer);
	}

	public Action<T, T> currentWindow() {
		return currentWindow;
	}

	protected void createWindowStream() {
		currentWindow = Action.<T>passthrough(dispatcher, capacity).env(environment);
	}

	@Override
	protected void doError(Throwable ev) {
		super.doError(ev);
		if(currentWindow != null)
			currentWindow.broadcastError(ev);
	}

	@Override
	protected void doComplete() {
		super.doComplete();
		if(currentWindow != null) {
			currentWindow.broadcastComplete();
			currentWindow = null;
		}
	}

	@Override
	protected void firstCallback(T event) {
		createWindowStream();
		//avoid that the next signal are dropped
		broadcastNext(currentWindow.onOverflowBuffer());
	}

	@Override
	protected void nextCallback(T event) {
		currentWindow.broadcastNext(event);
	}

	@Override
	protected void flushCallback(T event) {
		if(currentWindow != null){
			currentWindow.broadcastComplete();
			currentWindow = null;
		}
	}


}
