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
package reactor.rx;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.queue.CompletableConcurrentLinkedQueue;
import reactor.queue.CompletableQueue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Relationship between a Stream (Publisher) and a Subscriber.
 * <p>
 * In Reactor, a subscriber can be an Action which is both a Stream (Publisher) and a Subscriber.
 *
 * @author Stephane Maldini
 * @since 2.0
 */
public class StreamSubscription<O> implements Subscription {
	final           Subscriber<O>       subscriber;
	final           Stream<O>           publisher;
	protected final AtomicLong          capacity;
	protected final ReentrantLock       bufferLock;
	protected final CompletableQueue<O> buffer;

	public StreamSubscription(Stream<O> publisher, Subscriber<O> subscriber) {
		this(publisher, subscriber, new CompletableConcurrentLinkedQueue<O>());
	}

	public StreamSubscription(Stream<O> publisher, Subscriber<O> subscriber, CompletableQueue<O> buffer) {
		this.subscriber = subscriber;
		this.publisher = publisher;
		this.capacity = new AtomicLong();
		this.buffer = buffer;
		if (buffer != null) {
			bufferLock = new ReentrantLock();
		} else {
			bufferLock = null;
		}
	}

	@Override
	public void request(int elements) {
		if (buffer.isComplete() && buffer.isEmpty()) {
			return;
		}

		checkRequestSize(elements);

		int i = 0;
		O element;
		bufferLock.lock();
		try {
			while (i < elements && (element = buffer.poll()) != null) {
				subscriber.onNext(element);
				i++;
			}

			if (buffer.isComplete()) {
				onComplete();
			}

			if (i < elements) {
				capacity.getAndAdd(elements - i);
			}
		} finally {
			bufferLock.unlock();
		}
	}

	@Override
	public void cancel() {
		publisher.removeSubscription(this);
		buffer.clear();
		buffer.complete();
	}

	public void onNext(O ev) {
		if (capacity.getAndDecrement() > 0) {
			subscriber.onNext(ev);
		} else {
			bufferLock.lock();
			try {
				// we just decremented below 0 so increment back one
				if (capacity.incrementAndGet() > 0) {
					onNext(ev);
				} else{
					buffer.add(ev);
				}
			} finally {
				bufferLock.unlock();
			}

		}
	}

	public void onComplete() {
		if (buffer.isEmpty()) {
			subscriber.onComplete();
		}
		buffer.complete();
	}

	protected void checkRequestSize(int elements) {
		if (elements <= 0) {
			throw new IllegalArgumentException("Cannot request a non strictly positive number: " + elements);
		}
	}

	public void onError(Throwable throwable) {
		subscriber.onError(throwable);
	}

	public Stream<?> getPublisher() {
		return publisher;
	}

	public Subscriber<O> getSubscriber() {
		return subscriber;
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

	public boolean isComplete() {
		return buffer.isComplete();
	}

	@Override
	public int hashCode() {
		int result = subscriber.hashCode();
		result = 31 * result + publisher.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		StreamSubscription that = (StreamSubscription) o;

		if (publisher.hashCode() != that.publisher.hashCode()) return false;
		if (!subscriber.equals(that.subscriber)) return false;

		return true;
	}

	@Override
	public String toString() {
		return "{" +
				"capacity=" + capacity +
				", buffered=" + buffer.size() +
				'}';
	}

	public static class Firehose<O> extends StreamSubscription<O> {
		protected volatile boolean terminated = false;

		public Firehose(Stream<O> publisher, Subscriber<O> subscriber) {
			super(publisher, subscriber, null);
			capacity.set(Long.MAX_VALUE);
		}

		@Override
		public void request(int elements) {
		}

		@Override
		public void cancel() {
			publisher.removeSubscription(this);
			terminated = true;
		}

		@Override
		public void onComplete() {
			if (!terminated) {
				subscriber.onComplete();
			}
			terminated = true;
		}

		@Override
		public void onNext(O ev) {
			if (!terminated) {
				subscriber.onNext(ev);
			}
		}

		@Override
		public boolean isComplete() {
			return terminated;
		}

		@Override
		public String toString() {
			return "{" +
					"firehose!" +
					'}';
		}
	}
}
