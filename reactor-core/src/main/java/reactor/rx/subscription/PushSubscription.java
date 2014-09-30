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
import org.reactivestreams.Subscription;
import reactor.queue.CompletableQueue;
import reactor.rx.Stream;

/**
 * Relationship between a Stream (Publisher) and a Subscriber.
 * <p>
 * In Reactor, a subscriber can be an Action which is both a Stream (Publisher) and a Subscriber.
 *
 * @author Stephane Maldini
 */
public class PushSubscription<O> implements Subscription {
	final Subscriber<? super O> subscriber;
	final Stream<O>             publisher;

	protected volatile boolean terminated = false;

	public PushSubscription(Stream<O> publisher, Subscriber<? super O> subscriber) {
		this.subscriber = subscriber;
		this.publisher = publisher;
	}

	@Override
	public void request(long elements) {
		//IGNORE, full push
	}

	@Override
	public void cancel() {
		publisher.cleanSubscriptionReference(this);
		terminated = true;
	}

	public void onComplete() {
		if (!terminated) {
			subscriber.onComplete();
			terminated = true;
		}
	}

	public void onNext(O ev) {
		if (!terminated) {
			subscriber.onNext(ev);
		}
	}

	public void onError(Throwable throwable) {
		subscriber.onError(throwable);
	}

	public Stream<?> getPublisher() {
		return publisher;
	}

	public Subscriber<? super O> getSubscriber() {
		return subscriber;
	}

	public boolean isComplete() {
		return terminated;
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

		PushSubscription that = (PushSubscription) o;

		if (publisher.hashCode() != that.publisher.hashCode()) return false;
		if (!subscriber.equals(that.subscriber)) return false;

		return true;
	}

	@Override
	public String toString() {
		return "{push!}";
	}

	/**
	 * Wrap the subscription behind a reactive subscription using the passed queue to buffer otherwise to drop rejected
	 * data.
	 *
	 * @param queue the optional queue to buffer overflow
	 * @return the new ReactiveSubscription
	 */
	public ReactiveSubscription<O> toReactiveSubscription(CompletableQueue<O> queue) {
		final PushSubscription<O> thiz = this;
		return new WrappedReactiveSubscription<O>(thiz, queue);
	}

	static class WrappedReactiveSubscription<O> extends ReactiveSubscription<O> {
		final PushSubscription<O> thiz;

		public WrappedReactiveSubscription(final PushSubscription<O> thiz, CompletableQueue<O> queue) {
			super(thiz.publisher, new Subscriber<O>() {
				@Override
				public void onSubscribe(Subscription s) {
				}

				@Override
				public void onNext(O o) {
					thiz.onNext(o);
				}

				@Override
				public void onError(Throwable t) {
					thiz.onError(t);
				}

				@Override
				public void onComplete() {
					thiz.onComplete();
				}
			}, queue);
			this.thiz = thiz;
		}

		@Override
		public void request(long elements) {
			super.request(elements);
			thiz.request(elements);
		}

		@Override
		public void cancel() {
			super.cancel();
			thiz.cancel();
		}

		@Override
		public boolean equals(Object o) {
			return !(o == null || thiz.getClass() != o.getClass()) && thiz.equals(o);
		}

		@Override
		public int hashCode() {
			return thiz.hashCode();
		}
	}
}
