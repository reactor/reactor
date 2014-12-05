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
import reactor.rx.Stream;
import reactor.rx.subscription.ReactiveSubscription;
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

	private ReactiveSubscription<T> currentWindow;

	public WindowAction(Dispatcher dispatcher, int backlog) {
		super(dispatcher, backlog, true, true, true);
	}

	public WindowAction(Dispatcher dispatcher, int backlog, long timespan, TimeUnit unit, Timer timer) {
		super(dispatcher, backlog, true, true, true, timespan, unit, timer);
	}

	public ReactiveSubscription<T> currentWindow() {
		return currentWindow;
	}

	protected Stream<T> createWindowStream() {
		Action<T,T> action = Action.<T>passthrough(dispatcher, capacity).env(environment);
		ReactiveSubscription<T> _currentWindow = new ReactiveSubscription<T>(null, action);
		currentWindow = _currentWindow;
		action.onSubscribe(_currentWindow);
		return action;
	}

	@Override
	protected void doError(Throwable ev) {
		super.doError(ev);
		if (currentWindow != null)
			currentWindow.onError(ev);
	}

	@Override
	protected void doComplete() {
		super.doComplete();
		if (currentWindow != null) {
			currentWindow.onComplete();
			currentWindow = null;
		}
	}

	@Override
	protected void firstCallback(T event) {
		broadcastNext(createWindowStream());
	}

	@Override
	protected void nextCallback(T event) {
		currentWindow.onNext(event);
	}

	@Override
	protected void flushCallback(T event) {
		if (currentWindow != null) {
			currentWindow.onComplete();
			currentWindow = null;
		}
	}


}
