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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class TimeoutAction<T> extends Action<T, T> {

	private final Timer        timer;
	private final long         timeout;
	private final Action<?, T> parentAction;
	private final Consumer<Long> timeoutTask    = new Consumer<Long>() {
		@Override
		public void accept(Long aLong) {
			if (timeoutRegistration.getObject() == this)
				dispatch(timeoutRequest);
		}
	};
	private final Consumer<Void> timeoutRequest = new Consumer<Void>() {
		@Override
		public void accept(Void aVoid) {
			if (parentAction != null) {
					parentAction.resume();
					numbTimeout++;
				timeoutRegistration = timer.submit(timeoutTask, timeout, TimeUnit.MILLISECONDS);
			} else {
				doError(new TimeoutException("No data signaled for " + timeout + "ms"));
			}
		}
	};

	private final Consumer<Long> upstreamRequest = new Consumer<Long>() {
		@Override
		public void accept(Long integer) {
			timeoutRegistration.cancel();
			timeoutRegistration = timer.submit(timeoutTask, timeout, TimeUnit.MILLISECONDS);
			requestConsumer.accept(integer);
			if(integer == Long.MAX_VALUE) pendingNextSignals = 1;
		}
	};

	private volatile Registration<? extends Consumer<Long>> timeoutRegistration;
	private long numbTimeout = 0;

	@SuppressWarnings("unchecked")
	public TimeoutAction(Dispatcher dispatcher,
	                     Action<?, T> parentAction, Timer timer, long timeout) {
		super(dispatcher);
		Assert.state(timer != null, "Timer must be supplied");
		this.timer = timer;
		this.timeout = timeout;
		this.parentAction = parentAction;
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		super.doSubscribe(subscription);
		timeoutRegistration = timer.submit(timeoutTask, timeout, TimeUnit.MILLISECONDS);
	}

	@Override
	protected void doNext(T ev) {
		timeoutRegistration.cancel();
		numbTimeout = 0;
		broadcastNext(ev);
		timeoutRegistration = timer.submit(timeoutTask, timeout, TimeUnit.MILLISECONDS);
	}

	@Override
	protected void onRequest(long n) {
		dispatch(n, upstreamRequest);
	}

	@Override
	public Action<T, T> cancel() {
		if(timeoutRegistration != null){
			timeoutRegistration.cancel();
		}
		return super.cancel();
	}

	@Override
	public Action<T, T> pause() {
		if(timeoutRegistration != null){
			timeoutRegistration.pause();
		}
		return super.pause();
	}

	@Override
	public Action<T, T> resume() {
		if(timeoutRegistration != null){
			timeoutRegistration.resume();
		}
		return super.resume();
	}

	public long numberTimeouts() {
		return numbTimeout;
	}

	@Override
	public void doComplete() {
		timeoutRegistration.cancel();
		super.doComplete();
	}

	@Override
	public String toString() {
		return super.toString() + "{"
				+ "timeouts-since-data=" + numbTimeout
				+ "}";
	}
}
