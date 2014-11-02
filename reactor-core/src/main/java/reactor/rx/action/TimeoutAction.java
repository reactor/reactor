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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.event.registry.Registration;
import reactor.function.Consumer;
import reactor.timer.Timer;
import reactor.util.Assert;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class TimeoutAction<T> extends Action<T, T> {

	private final Timer                  timer;
	private final long                   timeout;
	private final Publisher<? extends T> fallback;
	private boolean switched = false;
	private long pendingRequests = 0l;

	private final Consumer<Long> timeoutTask = new Consumer<Long>() {
		@Override
		public void accept(Long aLong) {
			if (timeoutRegistration.getObject() == this)
				dispatch(timeoutRequest);
		}
	};

	private final Consumer<Void> timeoutRequest = new Consumer<Void>() {
		@Override
		public void accept(Void aVoid) {
			if (!timeoutRegistration.isCancelled()) {
				if (fallback != null) {
					TimeoutAction.this.cancel();
					switched = true;
					fallback.subscribe(TimeoutAction.this);

					if(pendingRequests > 0){
						upstreamSubscription.request(pendingRequests);
					}
				} else {
					doError(new TimeoutException("No data signaled for " + timeout + "ms"));
				}
			}
		}
	};

	private volatile Registration<? extends Consumer<Long>> timeoutRegistration;

	public TimeoutAction(Dispatcher dispatcher, Publisher<? extends T> fallback, Timer timer, long timeout) {
		super(dispatcher);
		Assert.state(timer != null, "Timer must be supplied");
		this.timer = timer;
		this.fallback = fallback;
		this.timeout = timeout;
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		super.doSubscribe(subscription);
		timeoutRegistration = timer.submit(timeoutTask, timeout, TimeUnit.MILLISECONDS);
	}

	@Override
	protected void requestUpstream(AtomicLong capacity, boolean terminated, long elements) {
		if((pendingRequests += elements) > 0) pendingRequests = Long.MAX_VALUE;
		super.requestUpstream(capacity, terminated, elements);
	}

	@Override
	protected void doNext(T ev) {
		if (switched) {
			broadcastNext(ev);
		} else {
			timeoutRegistration.cancel();
			broadcastNext(ev);
			timeoutRegistration = timer.submit(timeoutTask, timeout, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public Action<T, T> cancel() {
		if (timeoutRegistration != null) {
			timeoutRegistration.cancel();
			timeoutRegistration = null;
		}
		return super.cancel();
	}

	@Override
	public Action<T, T> pause() {
		if (timeoutRegistration != null) {
			timeoutRegistration.pause();
		}
		return super.pause();
	}

	@Override
	public Action<T, T> resume() {
		if (timeoutRegistration != null) {
			timeoutRegistration.resume();
		}
		return super.resume();
	}

	@Override
	public void doComplete() {
		if (timeoutRegistration != null) {
			timeoutRegistration.cancel();
			timeoutRegistration = null;
		}
		super.doComplete();
	}
}
