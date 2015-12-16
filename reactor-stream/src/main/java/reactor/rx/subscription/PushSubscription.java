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
package reactor.rx.subscription;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.fn.Consumer;
import reactor.rx.Stream;

/**
 * Relationship between a Stream (Publisher) and a Subscriber. A PushSubscription offers common facilities to track
 * downstream demand. Subclasses such as ReactiveSubscription implement these mechanisms to prevent Subscriber overrun.
 * <p>
 * In Reactor, a subscriber can be an Action which is both a Stream (Publisher) and a Subscriber.
 *
 * @author Stephane Maldini
 */
public class PushSubscription<O> implements Subscription, Consumer<Long>, ReactiveState.Upstream {
	protected final Subscriber<? super O> subscriber;
	protected final Stream<O>             publisher;

	protected volatile int terminated = 0;

	protected static final AtomicIntegerFieldUpdater<PushSubscription> TERMINAL_UPDATER = AtomicIntegerFieldUpdater
	  .newUpdater(PushSubscription.class, "terminated");


	protected volatile long pendingRequestSignals = 0l;

	protected static final AtomicLongFieldUpdater<PushSubscription> PENDING_UPDATER = AtomicLongFieldUpdater
	  .newUpdater(PushSubscription.class, "pendingRequestSignals");


	public PushSubscription(Stream<O> publisher, Subscriber<? super O> subscriber) {
		this.subscriber = subscriber;
		this.publisher = publisher;
	}

	@Override
	public Object upstream() {
		return publisher;
	}

	@Override
	public final void accept(Long n) {
		request(n);
	}

	@Override
	public void request(long n) {
		try {
			if (publisher == null) {
				BackpressureUtils.getAndAdd(PENDING_UPDATER, this, n);
			}

			if (terminated == -1L) {
				pendingRequestSignals = n;
				return;
			}

			onRequest(n);
		} catch (Throwable t) {
			onError(t);
		}

	}

	@Override
	public void cancel() {
		TERMINAL_UPDATER.set(this, 1);
		if (publisher != null) {
			//publisher.cancelSubscription(this);
		}
	}

	public void onComplete() {
		if (TERMINAL_UPDATER.compareAndSet(this, 0, 1) && subscriber != null) {
			subscriber.onComplete();
		}
	}

	public void onNext(O ev) {
		//	if (terminated == 0) {
		subscriber.onNext(ev);
		//	}
	}

	public void onError(Throwable throwable) {
		if (TERMINAL_UPDATER.compareAndSet(this, 0, 1) && subscriber != null) {
			subscriber.onError(throwable);
		}
	}

	public void start() {
		if (subscriber != null && TERMINAL_UPDATER.compareAndSet(this, -1, 0)) {
			subscriber.onSubscribe(this);
			long toRequest;
			synchronized (this) {
				if (!markAsStarted() || (toRequest = pendingRequestSignals) <= 0L) {
					return;
				}
			}
			onRequest(toRequest);
		}
	}

	public final boolean markAsStarted() {
		return TERMINAL_UPDATER.compareAndSet(this, -1, 0);
	}

	protected void onRequest(long n) {
		//IGNORE, full push
	}

	public final Subscriber<? super O> getSubscriber() {
		return subscriber;
	}

	public boolean isComplete() {
		return terminated == 1;
	}

	@Override
	public int hashCode() {
		int result = subscriber.hashCode();
		if (publisher != null) {
			result = 31 * result + publisher.hashCode();
		}
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		PushSubscription that = (PushSubscription) o;

		if (publisher != null && publisher.hashCode() != that.publisher.hashCode()) return false;
		if (!subscriber.equals(that.subscriber)) return false;

		return true;
	}

	@Override
	public String toString() {
		return "{push" +
		  (pendingRequestSignals > 0 && pendingRequestSignals != Long.MAX_VALUE ? ",pending=" + pendingRequestSignals :
			"")
		  + "}";
	}


}
