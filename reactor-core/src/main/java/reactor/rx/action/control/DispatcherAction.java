/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.rx.action.control;

import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.fn.Consumer;
import reactor.rx.action.Action;
import reactor.rx.subscription.PushSubscription;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public final class DispatcherAction<T> extends Action<T, T> {

	private final Dispatcher dispatcher;

	private volatile long pendingRequests = 0l;

	private final AtomicLongFieldUpdater<DispatcherAction> PENDING_UPDATER =
			AtomicLongFieldUpdater.newUpdater(DispatcherAction.class, "pendingRequests");


	public DispatcherAction(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	@Override
	public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
		return this.dispatcher != dispatcher;
	}

	@Override
	protected void doSubscribe(Subscription subscription) {
		long toRequest = PENDING_UPDATER.getAndSet(this, 0l);
		if (toRequest > 0l) {
			requestMore(toRequest);
		}
	}



	@Override
	protected void doStart(long pending) {
		//
	}

	@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		requestMore(elements);
	}

	@Override
	public void requestMore(long n) {
		Action.checkRequest(n);
		long toRequest = n != Long.MAX_VALUE ? Math.min(capacity, n) : Long.MAX_VALUE;
		PushSubscription<T> upstreamSubscription = this.upstreamSubscription;

		if (upstreamSubscription != null) {
			toRequest = toRequest - Math.max(upstreamSubscription.pendingRequestSignals(), 0l);
			toRequest = toRequest < 0l ? 0l : toRequest;

			if (n == Long.MAX_VALUE || PENDING_UPDATER.addAndGet(this, n - toRequest) < 0l) {
				PENDING_UPDATER.set(this, Long.MAX_VALUE);
			}

			if (toRequest > 0) {
					upstreamSubscription.accept(toRequest);
			}
		} else {
			if (n == Long.MAX_VALUE || PENDING_UPDATER.addAndGet(this, n) < 0l) {
				PENDING_UPDATER.set(this, Long.MAX_VALUE);
			}
		}

	}


	/*
	@Override
	public void requestMore(final long n) {
		checkRequest(n);
		try{
			dispatcher.tryDispatch(n, upstreamSubscription, null);
		}catch(InsufficientCapacityException s){
			Environment environment = getEnvironment();
			environment = environment == null && Environment.alive() ? Environment.get() : null;
			if(environment != null){
				environment.getTimer().submit(new Consumer<Long>() {
					@Override
					public void accept(Long aLong) {
						dispatcher.tryDispatch(n, upstreamSubscription, null);
					}
				});
			}
		}
	}*/

/*
	@Override
	protected void doStart(final long n) {
		if(dispatcher.inContext()){
			super.doStart(n);
		} else {
			dispatcher.dispatch(null, new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					DispatcherAction.super.doStart(n);
				}
			}, null);
		}
	}*/

	@Override
	public void onNext(T ev) {
		if (dispatcher.inContext()) {
			super.onNext(ev);
		} else {
			dispatcher.dispatch(ev, this, null);
		}
	}

	@Override
	public void onError(Throwable cause) {
		if (dispatcher.inContext()) {
			super.onError(cause);
		} else {
			dispatcher.dispatch(cause, new Consumer<Throwable>() {
				@Override
				public void accept(Throwable throwable) {
					DispatcherAction.super.onError(throwable);
				}
			}, null);
		}
	}

	@Override
	public void onComplete() {
		if (dispatcher.inContext()) {
			super.onComplete();
		} else {
			dispatcher.dispatch(null, new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					DispatcherAction.super.onComplete();
				}
			}, null);
		}
	}

	@Override
	protected void doNext(T ev) {
		broadcastNext(ev);
		long toRequest;
		if (pendingRequests != Long.MAX_VALUE &&
				upstreamSubscription.pendingRequestSignals() == 0l &&
				(toRequest = PENDING_UPDATER.getAndSet(this, 0l)) > 0l) {
			requestMore(toRequest);
		}
	}

	@Override
	public Dispatcher getDispatcher() {
		return dispatcher;
	}

	@Override
	public String toString() {
		return super.toString() + "{overflow=" + pendingRequests + "}";
	}
}
