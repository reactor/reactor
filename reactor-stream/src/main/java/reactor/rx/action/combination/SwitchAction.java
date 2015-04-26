/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.rx.action.combination;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.reactivestreams.SerializedSubscriber;
import reactor.core.support.NonBlocking;
import reactor.rx.action.Action;
import reactor.rx.action.support.DefaultSubscriber;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class SwitchAction<T> extends Action<Publisher<? extends T>, T> {

	private final SerializedSubscriber<T> serialized;
	private final Dispatcher              dispatcher;

	private long pendingRequests = 0l;
	private SwitchSubscriber switchSubscriber;

	public SwitchAction(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
		if (dispatcher != SynchronousDispatcher.INSTANCE) {
			serialized = null;
		} else {
			serialized = SerializedSubscriber.create(new DefaultSubscriber<T>() {
				@Override
				public void onNext(T t) {
					broadcastNext(t);
				}

				@Override
				public void onError(Throwable t) {
					broadcastError(t);
				}

				@Override
				public void onComplete() {
					broadcastComplete();
				}
			});
		}
	}

	public SwitchSubscriber getSwitchSubscriber() {
		return switchSubscriber;
	}

	@Override
	protected void doNext(Publisher<? extends T> ev) {
		SwitchSubscriber subscriber, nextSubscriber;
		synchronized (this) {
			pendingRequests--;
			subscriber = switchSubscriber;
			switchSubscriber = nextSubscriber= new SwitchSubscriber();
		}

		if (subscriber != null) {
			subscriber.cancel();
		}

		ev.subscribe(nextSubscriber);
	}

	@Override
	protected void doShutdown() {
		SwitchSubscriber subscriber;
		synchronized (this) {
			subscriber = switchSubscriber;
			if (subscriber != null) {
				switchSubscriber = null;
			}
		}
		if (subscriber != null) {
			subscriber.cancel();
		}
		super.doShutdown();
	}

	@Override
	protected void doComplete() {
		SwitchSubscriber subscriber;
		synchronized (this) {
			subscriber = switchSubscriber;
		}
		if (subscriber == null) {
			super.doComplete();
		} else {
			cancel();
		}
	}

	@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		SwitchSubscriber subscriber;
		synchronized (this) {
			if ((pendingRequests += elements) < 0) pendingRequests = Long.MAX_VALUE;
			subscriber = switchSubscriber;
		}
		super.requestUpstream(capacity, terminated, elements);
		if (subscriber != null) {
			subscriber.request(elements);
		}
	}

	@Override
	public final Dispatcher getDispatcher() {
		return dispatcher;
	}

	public class SwitchSubscriber implements NonBlocking, Subscriber<T>, Subscription {
		Subscription s;

		@Override
		public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
			return SwitchAction.this.isReactivePull(dispatcher, producerCapacity);
		}

		@Override
		public long getCapacity() {
			return capacity;
		}

		@Override
		public void onSubscribe(final Subscription s) {
			this.s = s;
			long pending;
			synchronized (SwitchAction.this) {
				pending = pendingRequests;
			}

			if (pending > 0) {
				s.request(pending);
			}
		}

		@Override
		public void onNext(T t) {
			synchronized (SwitchAction.this) {
				if (pendingRequests > 0 && pendingRequests != Long.MAX_VALUE) {
					pendingRequests--;
				}
			}
			serialized.onNext(t);
		}

		@Override
		public void onError(Throwable t) {
			serialized.onError(t);
		}

		@Override
		public void onComplete() {
			synchronized (SwitchAction.this) {
				switchSubscriber = null;
			}

			cancel();
			if (upstreamSubscription == null) {
				serialized.onComplete();
			}
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		public void cancel() {
			if (s != null) {
				s.cancel();
			}
		}

		public Subscription getSubscription() {
			return s;
		}
	}
}
