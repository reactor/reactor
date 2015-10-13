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
package reactor.rx.broadcast;

import java.util.Queue;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Timers;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.support.SignalType;
import reactor.fn.timer.Timer;
import reactor.rx.action.Action;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.ReactiveSubscription;

/**
 * A {@code Broadcaster} is a subclass of {@code Stream} which exposes methods for publishing values into the pipeline.
 * It is possible to publish discreet values typed to the generic type of the {@code Stream} as well as error conditions
 * and the Reactive Streams "complete" signal via the {@link #onComplete()} method.
 *
 * @author Stephane Maldini
 */
public class Broadcaster<O> extends Action<O, O> {

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link reactor.rx.action
	 * .Broadcaster#onNext(Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 *
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link reactor.rx.broadcast.Broadcaster}
	 */
	public static <T> Broadcaster<T> create() {
		return new Broadcaster<T>(null, false);
	}


	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link
	 * Broadcaster#onNext(Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 *
	 * @param timer the Reactor {@link reactor.fn.timer.Timer} to use downstream
	 * @param <T>   the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> create(Timer timer) {
		return new Broadcaster<T>(timer, false);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link
	 * Broadcaster#onNext(Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 * <p>
	 * Will not bubble up  any {@link CancelException}
	 *
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> passthrough() {
		return new Broadcaster<T>(null, true);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link
	 * Broadcaster#onNext(Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 * <p>
	 * Will not bubble up  any {@link CancelException}
	 *
	 * @param timer the Reactor {@link reactor.fn.timer.Timer} to use downstream
	 * @param <T>   the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> passthrough(Timer timer) {
		return new Broadcaster<T>(timer, true);
	}

	/**
	 * INTERNAL
	 */

	private final Timer   timer;
	private final boolean ignoreDropped;

	@SuppressWarnings("unchecked")
	protected Broadcaster(Timer timer, boolean ignoreDropped) {
		super();
		this.timer = timer;
		this.ignoreDropped = ignoreDropped;

		//start broadcaster
		SUBSCRIPTION.lazySet(this, SignalType.NOOP_SUBSCRIPTION);
	}


	@Override
	protected void doNext(O ev) {
		broadcastNext(ev);
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		if (SUBSCRIPTION.compareAndSet(this, SignalType.NOOP_SUBSCRIPTION, null)) {
			super.onSubscribe(subscription);

			PushSubscription<O> downSub = downstreamSubscription;
			if (downSub != null && downSub.pendingRequestSignals() > 0L) {
				subscription.request(downSub.pendingRequestSignals());
			}

		} else {
			super.onSubscribe(subscription);
		}
	}

	@Override
	protected PushSubscription<O> createSubscription(Subscriber<? super O> subscriber, Queue<O> queue) {
		if (queue != null) {
			return new ReactiveSubscription<O>(this, subscriber, queue) {

				@Override
				protected void onRequest(long elements) {
					if (upstreamSubscription != null) {
						super.onRequest(elements);
						requestUpstream(capacity, terminalSignalled, elements);
					}
				}
			};
		}
		else {
			return super.createSubscription(subscriber, null);
		}
	}

	@Override
	public void onNext(O ev) {
		try {
			super.onNext(ev);
		} catch (CancelException c) {
			if (!ignoreDropped) throw c;
		}
	}

	@Override
	protected void subscribeWithSubscription(Subscriber<? super O> subscriber, PushSubscription<O> subscription) {
		try {
			if (!addSubscription(subscription)) {
				subscriber.onError(new IllegalStateException("The subscription cannot be linked to this Stream"));
			} else {
				subscriber.onSubscribe(subscription);
			}
		} catch (Exception e) {
			Exceptions.throwIfFatal(e);
			subscriber.onError(e);
		}
	}

	@Override
	public Timer getTimer() {
		return timer != null ? timer : Timers.globalOrNull();
	}

	@Override
	public Broadcaster<O> capacity(long elements) {
		super.capacity(elements);
		return this;
	}

	@Override

	public void cancel() {
		Subscription parentSub = upstreamSubscription;
		if (parentSub != null && parentSub != SignalType.NOOP_SUBSCRIPTION) {
			super.cancel();
		}
	}

	@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		if (upstreamSubscription != null && upstreamSubscription != SignalType.NOOP_SUBSCRIPTION && !terminated) {
			requestMore(elements);
		} else {
			PushSubscription<O> _downstreamSubscription = downstreamSubscription;
			if (_downstreamSubscription != null && _downstreamSubscription.pendingRequestSignals() == 0L) {
				_downstreamSubscription.updatePendingRequests(elements);
			}
		}
	}


}
