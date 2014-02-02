/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.action;

import reactor.event.timer.HashWheelTimer;
import reactor.core.Observable;
import reactor.event.Event;
import reactor.event.lifecycle.Pausable;
import reactor.event.registry.Registration;
import reactor.function.Consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * WindowAction is collecting events on a steam until {@param period} is reached,
 * after that streams collected events further, clears the internal collection and
 * starts collecting items from empty list.
 *
 * @author Stephane Maldini
 */
public class WindowAction<T> extends Action<T> implements Pausable {

	private final ReentrantLock lock            = new ReentrantLock();
	private final List<T>       collectedWindow = new ArrayList<T>();
	private final Registration<? extends Consumer<Long>> timerRegistration;


	public WindowAction(Observable d,
	                    Object successKey,
	                    Object failureKey,
	                    HashWheelTimer timer,
	                    int period, TimeUnit timeUnit, int delay
  ) {
		super(d, successKey, failureKey);
		this.timerRegistration = timer.schedule(new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				doWindow(aLong);
			}
		}, period, timeUnit, delay);
	}

	protected void doWindow(Long aLong) {
		lock.lock();
		try {
			if(!collectedWindow.isEmpty()){
				notifyValue(Event.wrap(new ArrayList<T>(collectedWindow)));
				collectedWindow.clear();
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void doAccept(Event<T> value) {
		lock.lock();
		try {
			collectedWindow.add(value.getData());
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Pausable cancel() {
		timerRegistration.cancel();
		return this;
	}

	@Override
	public Pausable pause() {
		timerRegistration.pause();
		return this;
	}

	@Override
	public Pausable resume() {
		timerRegistration.resume();
		return this;
	}
}
