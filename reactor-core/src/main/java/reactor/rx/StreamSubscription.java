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

import org.reactivestreams.spi.Subscriber;
import org.reactivestreams.spi.Subscription;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Relationship between a Stream (Publisher) and a Subscriber.
 * <p/>
 * In Reactor, a subscriber can be an Action which is both a Stream (Publisher) and a Subscriber.
 *
 * @author Stephane Maldini
 * @since 1.1
 */
public class StreamSubscription<O> implements Subscription {
	final           Subscriber<O> subscriber;
	final           Stream<O>     publisher;
	protected final AtomicLong    capacity;
	final Queue<O> buffer = new ConcurrentLinkedQueue<O>();

	boolean terminated;


	public StreamSubscription(Stream<O> publisher, Subscriber<O> subscriber) {
		this.subscriber = subscriber;
		this.publisher = publisher;
		this.capacity = new AtomicLong();
		this.terminated = false;
	}

	@Override
	public void requestMore(int elements) {
		if (terminated && buffer.isEmpty()) {
			return;
		}

		if (elements <= 0) {
			throw new IllegalArgumentException("Cannot request negative number");
		}

		long currentCapacity = capacity.addAndGet(elements);
		long i = 0;
		O element;
		while (i++ < currentCapacity && (element = buffer.poll()) != null) {
			onNext(element);
		}
	}

	@Override
	public void cancel() {
		publisher.removeSubscription(this);
		buffer.clear();
		terminated = true;
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
	public int hashCode() {
		int result = subscriber.hashCode();
		result = 31 * result + publisher.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "{" +
				"capacity=" + capacity +
				'}';
	}

	public Stream<?> getPublisher() {
		return publisher;
	}
	public Subscriber<O> getSubscriber() {
		return subscriber;
	}

	public void onNext(O ev) {
		if (capacity.getAndDecrement() > 0) {
			subscriber.onNext(ev);
		} else {
			buffer.add(ev);
			// we just decremented below 0 so increment back one
			capacity.incrementAndGet();
		}
		if(terminated){
			onComplete();
		}
	}

	public void onComplete(){
		if(buffer.isEmpty()){
			subscriber.onComplete();
		}
		terminated = true;
	}

	public long getBufferSize() {
		return buffer.size();
	}

	public AtomicLong getCapacity() {
		return capacity;
	}
}
