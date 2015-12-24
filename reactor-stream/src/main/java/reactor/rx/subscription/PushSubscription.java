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
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
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
public class PushSubscription<O> implements Subscription, ReactiveState.Upstream,
                                            ReactiveState.ActiveDownstream, ReactiveState.ActiveUpstream, ReactiveState
		                                            .DownstreamDemand {
	protected final Subscriber<? super O> subscriber;
	protected final Stream<O>             publisher;

	private volatile int terminated = 0;

	protected static final AtomicIntegerFieldUpdater<PushSubscription> TERMINAL_UPDATER = AtomicIntegerFieldUpdater
	  .newUpdater(PushSubscription.class, "terminated");


	private volatile long pendingRequestSignals = 0;

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
		TERMINAL_UPDATER.compareAndSet(this, 0, -1);
	}

	public void onComplete() {
		if (TERMINAL_UPDATER.compareAndSet(this, 0, 1)) {
			subscriber.onComplete();
		}
	}

	public void onNext(O ev) {
		if (TERMINAL_UPDATER.get(this) == 0) {
			subscriber.onNext(ev);
		}
		else {
			throw CancelException.get();
		}
	}

	public void onError(Throwable throwable) {
		Exceptions.throwIfFatal(throwable);
		if (TERMINAL_UPDATER.compareAndSet(this, 0, 1)) {
			subscriber.onError(throwable);
		}
		else {
			throw ReactorFatalException.create(throwable);
		}
	}

	protected void onRequest(long n) {
		//IGNORE, full push
	}

	@Override
	public boolean isCancelled() {
		return terminated == -1;
	}

	@Override
	public long requestedFromDownstream() {
		return pendingRequestSignals;
	}

	@Override
	public boolean isTerminated() {
		return terminated == 1;
	}

	@Override
	public boolean isStarted() {
		return terminated == 0;
	}

	@Override
	public String toString() {
		return "{push" +
		  (pendingRequestSignals > 0 && pendingRequestSignals != Long.MAX_VALUE ? ",pending=" + pendingRequestSignals :
			"")
		  + "}";
	}


}
