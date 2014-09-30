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
	protected final AtomicLong          capacity;
	protected final ReentrantLock       bufferLock;
	protected final CompletableQueue<O> buffer;

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
		if (buffer.isComplete() && buffer.isEmpty()) {
			return;
		}

		Action.checkRequest(elements);

		int i = 0;
		O element;
		bufferLock.lock();

		while (i < elements && (element = buffer.poll()) != null) {
			bufferLock.unlock();
			subscriber.onNext(element);
			bufferLock.lock();
			i++;
		}

		if (buffer.isComplete()) {
			bufferLock.unlock();
			onComplete();
			return;
		}

		if (i < elements && (capacity.addAndGet(elements - i) < 0)) {
			bufferLock.unlock();
			onError(SpecificationExceptions.spec_3_17_exception(capacity.get(), elements));
			return;
		}

		bufferLock.unlock();

	}

	@Override
	public void cancel() {
		if (publisher != null) {
			publisher.cleanSubscriptionReference(this);
		}
		if (buffer != null) {
			bufferLock.lock();
			try {
				buffer.clear();
				buffer.complete();
			} finally {
				bufferLock.unlock();
			}

		}
	}

	@Override
	public void onNext(O ev) {
		if (capacity.getAndDecrement() > 0) {
			subscriber.onNext(ev);
		} else {
			bufferLock.lock();
			boolean retry = false;
			try {
				// we just decremented below 0 so increment back one
				if (capacity.incrementAndGet() > 0) {
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

	@Override
	public void onComplete() {
		if (buffer.isEmpty()) {
			subscriber.onComplete();
		}
		buffer.complete();
	}

	public long getBufferSize() {
		return buffer != null ? buffer.size() : -1l;
	}

	public AtomicLong getCapacity() {
		return capacity;
	}

	public CompletableQueue<O> getBuffer() {
		return buffer;
	}

	@Override
	public boolean isComplete() {
		return buffer.isComplete();
	}

	@Override
	public String toString() {
		return "{" +
				"capacity=" + capacity +
				", waiting=" + buffer.size() +
				'}';
	}
}
