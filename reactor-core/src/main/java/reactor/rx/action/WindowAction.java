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

/**
 * WindowAction is forwarding events on a steam until {@param backlog} is reached,
 * after that streams collected events further, complete it and create a fresh new stream.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public class WindowAction<T> extends BatchAction<T, Stream<T>> {

	private Action<T, T> currentWindow;

	@SuppressWarnings("unchecked")
	public WindowAction(Dispatcher dispatcher, long backlog) {
		super(backlog, dispatcher, true, true, true);
	}

	public Action<T, T> currentWindow() {
		return currentWindow;
	}

	protected void createWindowStream() {
		currentWindow = Action.<T>passthrough(dispatcher, capacity).env(environment);
		currentWindow.keepAlive(false);
	}

	@Override
	protected void doError(Throwable ev) {
		super.doError(ev);
		currentWindow.broadcastError(ev);
	}

	@Override
	protected void doComplete() {
		super.doComplete();
		currentWindow.broadcastComplete();
		currentWindow = null;
	}

	@Override
	protected void firstCallback(T event) {
		createWindowStream();
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
		}
	}


}
