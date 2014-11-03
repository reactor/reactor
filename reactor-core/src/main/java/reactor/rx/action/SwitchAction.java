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

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.rx.action.support.NonBlocking;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class SwitchAction<T> extends Action<Publisher<? extends T>, T> {

	private long pendingRequests = 0l;
	private SwitchSubscriber switchSubscriber;

	public SwitchAction(Dispatcher dispatcher) {
		super(dispatcher);
	}

	public SwitchSubscriber getSwitchSubscriber() {
		return switchSubscriber;
	}

	protected void doCleanCurrentSwitchSubscriber(){
		if(switchSubscriber != null){
			switchSubscriber.cancel();
			switchSubscriber = null;
		}
	}

	@Override
	protected void doNext(Publisher<? extends T> ev) {
		doCleanCurrentSwitchSubscriber();

		switchSubscriber = new SwitchSubscriber();
		ev.subscribe(switchSubscriber);
	}

	@Override
	protected void onShutdown() {
		doCleanCurrentSwitchSubscriber();
		super.onShutdown();
	}

	@Override
	protected void doComplete() {
		if(switchSubscriber == null){
			super.doComplete();
		}else{
			cancel();
		}
	}

	@Override
	protected void requestUpstream(AtomicLong capacity, boolean terminated, long elements) {
		if ((pendingRequests += elements) > 0) pendingRequests = Long.MAX_VALUE;
		super.requestUpstream(capacity, terminated, elements);
		if(switchSubscriber != null){
			switchSubscriber.request(elements);
		}
	}

	public class SwitchSubscriber implements NonBlocking, Subscriber<T>, Subscription, Consumer<T> {
		Subscription s;

		@Override
		public Dispatcher getDispatcher() {
			return dispatcher;
		}

		@Override
		public long getCapacity() {
			return capacity;
		}

		@Override
		public void onSubscribe(final Subscription s) {
			this.s = s;
			dispatch(new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					if(pendingRequests > 0){
						s.request(pendingRequests);
					}
				}
			});
		}

		@Override
		public void accept(T t) {
			if(pendingRequests > 0 && pendingRequests != Long.MAX_VALUE){
				pendingRequests--;
			}
			broadcastNext(t);
		}

		@Override
		public void onNext(T t) {
			trySyncDispatch(t, this);
		}

		@Override
		public void onError(Throwable t) {
			trySyncDispatch(t, new Consumer<Throwable>() {
				@Override
				public void accept(Throwable throwable) {
					broadcastError(throwable);
				}
			});
		}

		@Override
		public void onComplete() {
			trySyncDispatch(null, new Consumer<Void>() {
				@Override
				public void accept(Void nothing) {
					switchSubscriber = null;
					cancel();
					if(upstreamSubscription == null){
						broadcastComplete();
					}
				}
			});
		}

		@Override
		public void request(long n) {
			trySyncDispatch(n, new Consumer<Long>() {
				@Override
				public void accept(Long aLong) {
					s.request(aLong);
				}
			});
		}

		public void cancel() {
			s.cancel();
		}

		public Subscription getSubscription() {
			return s;
		}
	}
}
