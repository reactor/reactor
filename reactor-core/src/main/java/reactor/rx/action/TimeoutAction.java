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
import reactor.rx.Stream;
import reactor.timer.Timer;
import reactor.util.Assert;

import java.util.concurrent.TimeUnit;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class TimeoutAction<T> extends Action<T, T> {

	private final Timer     timer;
	private final long      timeout;
	private final Stream<?> flushedComposable;
	private final Consumer<Long> timeoutTask = new Consumer<Long>() {
		@Override
		public void accept(Long aLong) {
			if (timeoutRegistration == null || timeoutRegistration.getObject() == this) {
				flushedComposable.broadcastFlush();
			}
		}
	};

	private Registration<? extends Consumer<Long>> timeoutRegistration;

	@SuppressWarnings("unchecked")
	public TimeoutAction(Dispatcher dispatcher, Stream<?> flushedComposable,
	                     Timer timer, long timeout) {
		super(dispatcher);
		Assert.state(timer != null, "Timer must be supplied");
		this.timer = timer;
		this.timeout = timeout;
		this.flushedComposable = flushedComposable;
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		super.doSubscribe(subscription);
		timeoutRegistration = timer.submit(timeoutTask, timeout, TimeUnit.MILLISECONDS);
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
		timeoutRegistration = timer.submit(timeoutTask, timeout, TimeUnit.MILLISECONDS);
	}

	@Override
	public Action<T,T> cancel() {
		timeoutRegistration.cancel();
		return super.cancel();
	}

	@Override
	public Action<T,T> pause() {
		timeoutRegistration.pause();
		return super.pause();
	}

	@Override
	public Action<T,T> resume() {
		timeoutRegistration.resume();
		return super.resume();
	}

	@Override
	public void onComplete() {
		timeoutRegistration.cancel();
	}

}
