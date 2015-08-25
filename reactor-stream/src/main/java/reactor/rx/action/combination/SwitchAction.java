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
import reactor.ReactorProcessor;
import reactor.core.error.CancelException;
import reactor.core.subscriber.SerializedSubscriber;
import reactor.core.support.Bounded;
import reactor.core.error.Exceptions;
import reactor.rx.action.Action;
import reactor.rx.broadcast.Broadcaster;

/**
 * @author Stephane Maldini
 * @since 2.0
 */
public class SwitchAction<T> extends Action<Publisher<? extends T>, T> {

	private final ReactorProcessor dispatcher;

	private long pendingRequests = 0l;
	private SwitchSubscriber switchSubscriber;

	public SwitchAction(ReactorProcessor dispatcher) {
		this.dispatcher = dispatcher;
	}

	public SwitchSubscriber getSwitchSubscriber() {
		return switchSubscriber;
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		final SwitchSubscriber switcher;
		final boolean toSubscribe;
		synchronized (this) {
			switcher = switchSubscriber;
			toSubscribe = switcher != null && switcher.s == null;
		}
		if (toSubscribe) {
			switcher.publisher.subscribe(switcher);
		}
		super.subscribe(SerializedSubscriber.create(subscriber));
	}

	@Override
	public void onNext(Publisher<? extends T> ev) {
		if (ev == null) {
			throw new NullPointerException("Spec 2.13: Signal cannot be null");
		}

		try {
			doNext(ev);
		} catch (CancelException uae) {
			throw uae;
		} catch (Throwable cause) {
			doError(Exceptions.addValueAsLastCause(cause, ev));
		}
	}

	@Override
	public void cancel() {
		SwitchSubscriber subscriber;
		synchronized (this) {
			subscriber = switchSubscriber;
		}
		if (subscriber != null) {
			subscriber.cancel();
		}

		if (upstreamSubscription != Broadcaster.HOT_SUBSCRIPTION) {
			super.cancel();
		}
	}

	@Override
	protected void doNext(Publisher<? extends T> ev) {
		SwitchSubscriber subscriber, nextSubscriber;
		synchronized (this) {
			if(switchSubscriber != null && switchSubscriber.publisher == ev) return;
			if(pendingRequests != Long.MAX_VALUE) pendingRequests--;
			subscriber = switchSubscriber;
			switchSubscriber = nextSubscriber= new SwitchSubscriber(ev);
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
	public final ReactorProcessor getDispatcher() {
		return dispatcher;
	}

	public class SwitchSubscriber implements Bounded, Subscriber<T>, Subscription {
		final Publisher<? extends T> publisher;

		Subscription s;

		public SwitchSubscriber(Publisher<? extends T> publisher) {
			this.publisher = publisher;
		}

		@Override
		public boolean isExposedToOverflow(Bounded upstream) {
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

			if (pending > 0 && downstreamSubscription != null) {
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
			broadcastNext(t);
		}

		@Override
		public void onError(Throwable t) {
			broadcastError(t);
		}

		@Override
		public void onComplete() {
			synchronized (SwitchAction.this) {
				switchSubscriber = null;
			}

			cancel();
			if (upstreamSubscription == null) {
				broadcastComplete();
			}
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		public void cancel() {
			Subscription s = this.s;
			if (s != null) {
				this.s = null;
				s.cancel();
			}
		}

		public Subscription getSubscription() {
			return s;
		}
	}
}
