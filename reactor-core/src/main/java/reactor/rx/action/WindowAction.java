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
package reactor.rx.action;

import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.event.registry.Registration;
import reactor.function.Consumer;
import reactor.timer.Timer;
import reactor.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * WindowAction is collecting events on a steam until {@param period} is reached,
 * after that streams collected events further, clears the internal collection and
 * starts collecting items from empty list.
 *
 * @author Stephane Maldini
 * @since 1.1
 */
public class WindowAction<T> extends Action<T, List<T>> {

	private final List<T> collectedWindow = new ArrayList<T>();
	private final Registration<? extends Consumer<Long>> timerRegistration;

	private boolean terminated = false;


	@SuppressWarnings("unchecked")
	public WindowAction(Dispatcher dispatcher,
	                    Timer timer,
	                    int period, TimeUnit timeUnit, int delay
	) {
		super(dispatcher);
		Assert.state(timer != null, "Timer must be supplied");
		this.timerRegistration = timer.schedule(new Consumer<Long>() {
			@Override
			public void accept(Long aLong) {
				onWindow(aLong);
			}
		}, period, timeUnit, delay);
	}

	protected void onWindow(Long aLong) {
		reactor.function.Consumer<Subscription> completeHandler = new reactor.function.Consumer<Subscription>() {
			@Override
			public void accept(Subscription subscription) {
				if (!collectedWindow.isEmpty()) {
					broadcastNext(new ArrayList<T>(collectedWindow));
					collectedWindow.clear();

					if (terminated) {
						broadcastComplete();
					}
				}
				if (terminated) {
					timerRegistration.cancel();
				}
			}
		};
		dispatcher.dispatch(this, null, null, null, ROUTER, completeHandler);
	}

	@Override
	protected void doNext(T value) {
		collectedWindow.add(value);
	}

	@Override
	protected void doComplete() {
		if (collectedWindow.isEmpty()) {
			super.doComplete();
		} else {
			terminated = true;
		}
	}

	@Override
	public WindowAction<T> cancel() {
		timerRegistration.cancel();
		return (WindowAction<T>) super.cancel();
	}

	@Override
	public WindowAction<T> pause() {
		timerRegistration.pause();
		return (WindowAction<T>) super.pause();
	}

	@Override
	public WindowAction<T> resume() {
		timerRegistration.resume();
		return (WindowAction<T>) super.resume();
	}
}
