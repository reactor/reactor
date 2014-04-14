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
package reactor.core.composable.action;

import reactor.event.dispatch.Dispatcher;
import reactor.event.lifecycle.Pausable;
import reactor.event.registry.Registration;
import reactor.function.Consumer;
import reactor.timer.Timer;

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
 * @since 1.1
 */
public class WindowAction<T> extends Action<T,List<T>> implements Flushable<T> {

	private final ReentrantLock lock            = new ReentrantLock();
	private final List<T>       collectedWindow = new ArrayList<T>();
	private final Registration<? extends Consumer<Long>> timerRegistration;


	public WindowAction(Dispatcher dispatcher,
	                    ActionProcessor<List<T>> actionProcessor,
	                    Timer timer,
	                    int period, TimeUnit timeUnit, int delay
  ) {
		super(dispatcher, actionProcessor);
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
				output.onNext(new ArrayList<T>(collectedWindow));
				collectedWindow.clear();
			}
		} finally {
			lock.unlock();
		}
		available();
	}

	@Override
	public void doNext(T value) {
		lock.lock();
		try {
			collectedWindow.add(value);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public WindowAction<T> cancel() {
		timerRegistration.cancel();
		return (WindowAction<T>)super.cancel();
	}

	@Override
	public WindowAction<T> pause() {
		timerRegistration.pause();
		return (WindowAction<T>)super.pause();
	}

	@Override
	public WindowAction<T> resume() {
		timerRegistration.resume();
		return (WindowAction<T>)super.resume();
	}

	@Override
	public Flushable<T> flush() {
		doWindow(-1l);
		return this;
	}

	@Override
	protected void doComplete() {
		flush();
		super.doComplete();
	}
}
