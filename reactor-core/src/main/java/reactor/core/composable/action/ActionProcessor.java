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
package reactor.core.composable.action;

import org.reactivestreams.api.Producer;
import org.reactivestreams.spi.Publisher;
import org.reactivestreams.spi.Subscriber;
import org.reactivestreams.spi.Subscription;
import reactor.alloc.Recyclable;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class ActionProcessor<T> implements
		org.reactivestreams.spi.Subscriber<T>,
		org.reactivestreams.spi.Publisher<T>,
		org.reactivestreams.api.Consumer<T>,
		Flushable<T>,
		Recyclable {

	private static enum State {
		READY,
		ERROR,
		COMPLETE,
		SHUTDOWN;
	}

	private final Queue<T>                    buffer                = new ConcurrentLinkedQueue<T>();
	private final List<ActionSubscription<T>> subscriptions         = new ArrayList<ActionSubscription<T>>(8);
	private final List<Flushable<?>>          flushables            = new ArrayList<Flushable<?>>(8);
	private final List<Subscription>          upstreamSubscriptions = new ArrayList<Subscription>(1);

	private long    bufferSize;
	private boolean keepAlive;
	private int       remaining = 0;
	private State     state     = State.READY;
	private Throwable error     = null;

	public ActionProcessor(long bufferSize, boolean keepAlive) {
		this.bufferSize = bufferSize < 0l ? Long.MAX_VALUE : bufferSize;
		this.keepAlive = keepAlive;
	}

	public ActionProcessor(long bufferSize) {
		this(bufferSize, false);
	}

	@Override
	public Subscriber<T> getSubscriber() {
		return this;
	}

	@Override
	public void subscribe(final Subscriber<T> subscriber) {
		try {
			if (checkState()) {
				ActionSubscription<T> subscription = new ActionSubscription<T>(this, subscriber);

				if (subscriptions.indexOf(subscription) != -1) {
					subscriber.onError(new IllegalArgumentException("Subscription already exists between this " +
							"publisher/subscriber pair"));
				} else {
					subscriber.onSubscribe(subscription);
				}
			} else {
				if (state == State.COMPLETE) {
					subscriber.onComplete();
				} else if (state == State.SHUTDOWN) {
					subscriber.onError(new IllegalArgumentException("Publisher has shutdown"));
				} else {
					subscriber.onError(error);
				}
			}
		} catch (Throwable cause) {
			subscriber.onError(cause);
		}

	}

	@Override
	public void onSubscribe(Subscription subscription) {
		if (upstreamSubscriptions.indexOf(subscription) != -1)
			throw new IllegalArgumentException("This Processor is already subscribed");

		upstreamSubscriptions.add(subscription);
	}

	@Override
	public Flushable<T> flush() {
		if (!checkState()) return this;

		if (flushables.isEmpty()) return this;
		for (Flushable<?> subscriber : flushables) {
			try {
				subscriber.flush();
			} catch (Throwable throwable) {
				for (ActionSubscription<T> subscription : subscriptions) {
					if (subscription.subscriber == subscriber) {
						callError(subscription, throwable);
						break;
					}
				}
				onError(throwable);
			}
		}
		return this;
	}

	@Override
	public void onNext(T ev) {
		try {
			if (!checkState() || bufferEvent(ev)) return;

			if (subscriptions.isEmpty()) {
				buffer.add(ev);
				return;
			}

			for (ActionSubscription<T> subscription : subscriptions) {
				try {
					if (subscription.cancelled || subscription.completed || subscription.error)
						continue;

					subscription.subscriber.onNext(ev);
				} catch (Throwable throwable) {
					callError(subscription, throwable);
				}
			}
		} catch (Throwable unrecoverable) {
			state = State.ERROR;
			error = unrecoverable;
			for (ActionSubscription<T> subscription : subscriptions) {
				callError(subscription, unrecoverable);
			}
		}

		remaining--;
	}

	@Override
	public void onError(Throwable throwable) {
		if (!checkState()) return;

		if (subscriptions.isEmpty()) return;
		for (ActionSubscription<T> subscription : subscriptions) {
			callError(subscription, throwable);
		}
	}

	@Override
	public void onComplete() {
		if (!checkState()) return;

		state = State.COMPLETE;
		if (subscriptions.isEmpty()) return;

		for (ActionSubscription<T> subscription : subscriptions) {
			try {
				if (!subscription.completed) {
					subscription.subscriber.onComplete();
					subscription.completed = true;
				}
			} catch (Throwable throwable) {
				callError(subscription, throwable);
			}
		}
	}

	@Override
	public void recycle() {
		buffer.clear();
		subscriptions.clear();
		flushables.clear();
		upstreamSubscriptions.clear();
		state = State.READY;
		bufferSize = 0;
		keepAlive = false;
		remaining = 0;
		error = null;
	}

	public void subscribe(final Flushable<?> flushable) {
		flushables.add(flushable);
	}

	public void source(final Producer<T> source) {
		source(source.getPublisher());
	}

	public void source(final Publisher<T> source) {
		source.subscribe(this);
	}

	public List<ActionSubscription<T>> getSubscriptions() {
		return subscriptions;
	}

	public List<Flushable<?>> getFlushables() {
		return flushables;
	}

	public void setBufferSize(long bufferSize) {
		this.bufferSize = bufferSize;
	}

	public void setKeepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
	}

	public State getState() {
		return state;
	}

	public Throwable getError() {
		return error;
	}

	private void unsubscribe(ActionSubscription<T> tSubscription) {
		subscriptions.remove(tSubscription);
		if (subscriptions.isEmpty() && !keepAlive) {
			state = State.SHUTDOWN;
			if (!upstreamSubscriptions.isEmpty()) {
				for (Subscription subscription : upstreamSubscriptions) {
					subscription.cancel();
				}

			}
		}
	}

	private boolean bufferEvent(T next) {
		if (remaining > 0) {
			return false;
		}

		buffer.add(next);
		return true;
	}

	private void callError(ActionSubscription<T> subscription, Throwable cause) {
		if (!subscription.error) {
			subscription.subscriber.onError(cause);
			subscription.error = true;
		}
	}

	private boolean checkState() {
		return state != State.ERROR && state != State.COMPLETE && state != State.SHUTDOWN;

	}

	private void drain(int elements) {
		if (buffer.isEmpty()) return;

		int min = elements;
		for (ActionSubscription<T> subscription : subscriptions) {
			min = Math.min(subscription.lastRequested, min);
		}

		remaining = min;

		T data;
		int i = 0;
		while ((data = buffer.poll()) != null && i < min) {
			onNext(data);
			i++;
		}
	}

	private static class ActionSubscription<T> implements org.reactivestreams.spi.Subscription {
		private final Subscriber<T> subscriber;

		private final ActionProcessor<T> publisher;
		private       int                lastRequested;

		private boolean cancelled = false;
		private boolean completed = false;
		private boolean error     = false;

		public ActionSubscription(ActionProcessor<T> publisher, Subscriber<T> subscriber) {
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

			ActionSubscription that = (ActionSubscription) o;

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

	}
}