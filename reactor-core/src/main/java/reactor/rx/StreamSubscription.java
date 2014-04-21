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

/**
* @author Stephane Maldini
*/
public class StreamSubscription<O> implements org.reactivestreams.spi.Subscription {
	final Subscriber<O> subscriber;
	final Stream<O>  publisher;

	int lastRequested;
	boolean cancelled = false;
	boolean completed = false;
	boolean error     = false;

	public StreamSubscription(Stream<O> publisher, Subscriber<O> subscriber) {
		this.subscriber = subscriber;
		this.publisher = publisher;
	}

	@Override
	public void requestMore(int elements) {
		if (cancelled) {
			return;
		}

		if (elements <= 0) {
			throw new IllegalStateException("Cannot request negative number");
		}

		this.lastRequested = elements;

		publisher.drain(elements);
	}

	@Override
	public void cancel() {
		if (cancelled)
			return;

		cancelled = true;
		publisher.unsubscribe(this);
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
		return "" +
				(cancelled ? "cancelled" : (completed ? "completed" : (error ? "error" : lastRequested)))
				;
	}

	public Stream<?> getPublisher() {
		return publisher;
	}
}
