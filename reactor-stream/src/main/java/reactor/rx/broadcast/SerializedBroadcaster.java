/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.rx.broadcast;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.subscriber.SerializedSubscriber;
import reactor.fn.timer.Timer;

/**
 * A {@code Broadcaster} is a subclass of {@code Stream} which exposes methods for publishing values into the pipeline.
 * It is possible to publish discreet values typed to the generic type of the {@code Stream} as well as error conditions
 * and the Reactive Streams "complete" signal via the {@link #onComplete()} method.
 *
 * @author Stephane Maldini
 */
public final class SerializedBroadcaster<O> extends Broadcaster<O> {

	final private SerializedSubscriber<O> serializer;


	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link reactor.rx.action
	 * .Broadcaster#onNext(Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 * <p>
	 * A serialized broadcaster will make sure that even in a multhithreaded scenario, only one thread will be able to
	 * broadcast at a time.
	 * The synchronization is non blocking for the publisher, using thread-stealing and first-in-first-served patterns.
	 *
	 * @param <T> the type of values passing through the {@literal action}
	 * @return a new {@link reactor.rx.action.Action}
	 */
	public static <T> Broadcaster<T> create() {
		return new SerializedBroadcaster<>(null);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values, ready to broadcast values with {@link
	 * Broadcaster#onNext(Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 * <p>
	 * A serialized broadcaster will make sure that even in a multhithreaded scenario, only one thread will be able to
	 * broadcast at a time.
	 * The synchronization is non blocking for the publisher, using thread-stealing and first-in-first-served patterns.
	 *
	 * @param env the Reactor {@link reactor.fn.timer.Timer} to use
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> create(Timer env) {
		return new SerializedBroadcaster<>(env);
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		serializer.onSubscribe(subscription);
	}

	/**
	 * @see {@link org.reactivestreams.Subscriber#onNext(Object)}
	 */
	@Override
	public void onNext(O ev) {
		try {
			serializer.onNext(ev);
		} catch (Throwable cause) {
			doError(Exceptions.addValueAsLastCause(cause, ev));
		}
	}

	/**
	 * @see {@link org.reactivestreams.Subscriber#onError(Throwable)}
	 */
	@Override
	public void onError(Throwable ev) {
		serializer.onError(ev);
	}

	/**
	 * @see {@link org.reactivestreams.Subscriber#onComplete()}
	 */
	@Override
	public void onComplete() {
		serializer.onComplete();
	}


	/**
	 * Internal
	 */

	private SerializedBroadcaster(Timer timer) {
		super(timer, false);
		this.serializer = SerializedSubscriber.create(new Subscriber<O>() {
			@Override
			public void onSubscribe(Subscription s) {
				SerializedBroadcaster.super.onSubscribe(s);
			}

			@Override
			public void onNext(O o) {
				SerializedBroadcaster.super.doNext(o);
			}

			@Override
			public void onError(Throwable t) {
				SerializedBroadcaster.super.doError(t);
			}

			@Override
			public void onComplete() {
				SerializedBroadcaster.super.doComplete();
			}
		});
	}
}
