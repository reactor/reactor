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

import org.reactivestreams.Subscriber;
import reactor.queue.CompletableLinkedQueue;
import reactor.queue.CompletableQueue;
import reactor.rx.Stream;
import reactor.rx.action.Action;
import reactor.rx.action.support.SpecificationExceptions;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Relationship between a Stream (Publisher) and a Subscriber.
 * <p>
 * A Reactive Subscription using a pattern called "reactive-pull" to dynamically adapt to the downstream subscriber
 * capacity:
 * - If no capacity (no previous request or capacity drained), queue data into the buffer {@link CompletableQueue}
 * - If capacity (previous request and capacity remaining), call subscriber onNext
 * <p>
 * Queued data will be polled when the next request(n) signal is received. If there is remaining requested volume,
 * it will be added to the current capacity and therefore will let the next signals to be directly pushed.
 * Each next signal will decrement the capacity by 1.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public class ReactiveSubscription<O> extends PushSubscription<O> {

	//Shared between subscriber and publisher
	protected final AtomicLong capacity;

	//Shared between subscriber and publisher
	protected final ReentrantLock bufferLock;

	//Guarded by bufferLock
	protected final CompletableQueue<O> buffer;

	//Only read from subscriber context
	protected long currentNextSignals = 0l;

	//Can be set outside of publisher and subscriber contexts
	protected volatile long maxCapacity = Long.MAX_VALUE;

	public ReactiveSubscription(Stream<O> publisher, Subscriber<? super O> subscriber) {
		this(publisher, subscriber, new CompletableLinkedQueue<O>());
	}

	public ReactiveSubscription(Stream<O> publisher, Subscriber<? super O> subscriber, CompletableQueue<O> buffer) {
		super(publisher, subscriber);
		this.capacity = new AtomicLong();
		this.buffer = buffer;
		if (buffer != null) {
			bufferLock = new ReentrantLock();
		} else {
			bufferLock = null;
		}
	}

	@Override
	public void request(long elements) {
		Action.checkRequest(elements);

		//Subscription terminated, Buffer done, return immediately
		if (buffer.isComplete() && buffer.isEmpty()) {
			return;
		}

		//Fetch current pending requests
		long previous = pendingRequestSignals;

		//If unbounded request, set and return
		if (elements == Long.MAX_VALUE) {
			if (previous != 0) {
				capacity.set(maxCapacity);
				return;
			} else {
				pendingRequestSignals = Long.MAX_VALUE;
			}
		} else if (previous != Long.MAX_VALUE) {
			if ((pendingRequestSignals += elements) < 0l) {
				onError(SpecificationExceptions.spec_3_17_exception(subscriber, previous, elements));
				return;
			}
		}

		long toRequest = elements;
		toRequest = Math.min(toRequest, maxCapacity);

		try {

			int i;
			O element;
			bufferLock.lock();

			do {
				i = 0;
				currentNextSignals = 0;

				if (pendingRequestSignals != Long.MAX_VALUE) {
					pendingRequestSignals = toRequest > pendingRequestSignals ? 0 : pendingRequestSignals - toRequest;
				}

				while (i < toRequest && (element = buffer.poll()) != null) {
					subscriber.onNext(element);
					i++;
				}

				if (buffer.isComplete()) {
					onComplete();
					return;
				}

				if (capacity.addAndGet(toRequest - i) < -1l) {
					onError(SpecificationExceptions.spec_3_17_exception(subscriber, previous, elements));
					return;
				}

				onRequest(pendingRequestSignals == Long.MAX_VALUE ? Long.MAX_VALUE  : toRequest);

				toRequest = Math.min(pendingRequestSignals, maxCapacity);
			} while (toRequest > 0 && !buffer.isEmpty());

			bufferLock.unlock();

		} catch (Exception e) {
			if (bufferLock.isHeldByCurrentThread()) {
				bufferLock.unlock();
			}
			onError(e);
		}

	}

	@Override
	public void cancel() {
		if (publisher != null) {
			publisher.cleanSubscriptionReference(this);
		}
		if (buffer != null) {
			bufferLock.lock();
			try {
				//buffer.clear();
				buffer.complete();
			} finally {
				bufferLock.unlock();
			}

		}
	}

	@Override
	public long clearPendingRequest() {
		long _pendingRequestSignals = pendingRequestSignals;
		pendingRequestSignals = 0l;
		capacity.set(0l);
		return _pendingRequestSignals;
	}

	@Override
	public void onNext(O ev) {
		if (pendingRequestSignals == Long.MAX_VALUE || capacity.getAndDecrement() > 0) {
			subscriber.onNext(ev);
		} else {
			bufferLock.lock();
			boolean retry = false;
			try {
				// we just decremented below 0 so increment back one
				if (capacity.incrementAndGet() > 0 || pendingRequestSignals == Long.MAX_VALUE) {
					retry = true;
				} else {
					buffer.add(ev);
				}
			} finally {
				bufferLock.unlock();
			}

			if (retry) {
				onNext(ev);
			}

		}
	}

	public final long currentNextSignals() {
		return currentNextSignals;
	}

	@Override
	public void onComplete() {

		bufferLock.lock();
		try{
			if(terminated)
				return;
			if (buffer.isEmpty()) {
				terminated = true;
				subscriber.onComplete();
			}
			buffer.complete();
		}finally {
			bufferLock.unlock();
		}
	}

	@Override
	public final void incrementCurrentNextSignals() {
		currentNextSignals++;
	}

	@Override
	public boolean shouldRequestPendingSignals() {
		return pendingRequestSignals > 0 && pendingRequestSignals != Long.MAX_VALUE && currentNextSignals == maxCapacity;
	}

	public final long maxCapacity() {
		return maxCapacity;
	}

	@Override
	public final void maxCapacity(long maxCapacity) {
		this.maxCapacity = maxCapacity;
	}

	public final long getBufferSize() {
		return buffer != null ? buffer.size() : -1l;
	}

	public final AtomicLong capacity() {
		return capacity;
	}

	public final CompletableQueue<O> getBuffer() {
		return buffer;
	}

	@Override
	public final boolean isComplete() {
		return buffer.isComplete();
	}

	@Override
	public String toString() {
		long currentCapacity = capacity.get();
		return "{" +
				"capacity=" + (currentCapacity == Long.MAX_VALUE ? "infinite" : currentCapacity + "/" + maxCapacity
				+ " [" + (int) ((((float) currentCapacity) / (float) maxCapacity) * 100) + "%]") +
				", current=" + currentNextSignals +
				", pending=" + (pendingRequestSignals() == Long.MAX_VALUE ? "infinite" : pendingRequestSignals()) +
				(buffer != null ? ", waiting=" + buffer.size() : "") +
				'}';
	}
}
