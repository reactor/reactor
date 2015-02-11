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

import org.reactivestreams.Subscriber;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.queue.CompletableQueue;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.rx.action.Action;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.ReactiveSubscription;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * A {@code Broadcaster} is a subclass of {@code Stream} which exposes methods for publishing values into the pipeline.
 * It is possible to publish discreet values typed to the generic type of the {@code Stream} as well as error conditions
 * and the Reactive Streams "complete" signal via the {@link #onComplete()} method.
 *
 * @author Stephane Maldini
 */
public class Broadcaster<O> extends Action<O, O> {

	protected final Dispatcher  dispatcher;
	protected final Environment environment;

	private volatile long pendingRequests = 0l;

	private final AtomicLongFieldUpdater<Broadcaster> PENDING_UPDATER =
			AtomicLongFieldUpdater.newUpdater(Broadcaster.class, "pendingRequests");

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
		return new Broadcaster<T>(null, SynchronousDispatcher.INSTANCE, Long.MAX_VALUE);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link
	 * Broadcaster#onNext(Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 *
	 * @param env the Reactor {@link reactor.Environment} to use
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> create(Environment env) {
		return create(env, env.getDefaultDispatcher());
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link
	 * reactor.rx.action.Action#onNext(Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 *
	 * @param dispatcher the {@link reactor.core.Dispatcher} to use
	 * @param <T>        the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> create(Dispatcher dispatcher) {
		return create(null, dispatcher);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link Broadcaster#onNext
	 * (Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 *
	 * @param env        the Reactor {@link reactor.Environment} to use
	 * @param dispatcher the {@link reactor.core.Dispatcher} to use
	 * @param <T>        the type of values passing through the {@literal Stream}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> create(Environment env, Dispatcher dispatcher) {
		Assert.state(dispatcher.supportsOrdering(), "Dispatcher provided doesn't support event ordering. " +
				" For concurrent consume, refer to Stream#partition/groupBy() method and assign individual single " +
				"dispatchers");
		return new Broadcaster<T>(env, dispatcher, Action.evaluateCapacity(dispatcher.backlogSize()));
	}

	/**
	 * INTERNAL
	 */
	protected Broadcaster(Environment environment, Dispatcher dispatcher, long capacity) {
		super(capacity);
		this.dispatcher = dispatcher;
		this.environment = environment;
	}

	@Override
	public final Dispatcher getDispatcher() {
		return dispatcher;
	}

	@Override
	protected void doNext(O ev) {
		broadcastNext(ev);
		long toRequest;
		if (upstreamSubscription != null &&
				pendingRequests != Long.MAX_VALUE &&
				upstreamSubscription.pendingRequestSignals() == 0l &&
				(toRequest = PENDING_UPDATER.getAndSet(this, 0l)) > 0l) {
			requestMore(toRequest);
		}
	}

	@Override
	public void onNext(O ev) {
		if (!dispatcher.inContext()) {
			dispatcher.dispatch(ev, this, null);
		} else {
			super.onNext(ev);
		}
	}

	@Override
	public void onError(Throwable cause) {
		if (!dispatcher.inContext()) {
			dispatcher.dispatch(cause, new Consumer<Throwable>() {
				@Override
				public void accept(Throwable throwable) {
					Broadcaster.super.doError(throwable);
				}
			}, null);
		} else {
			super.onError(cause);
		}
	}

	@Override
	public void onComplete() {
		if (!dispatcher.inContext()) {
			dispatcher.dispatch(null, new Consumer<Void>() {
				@Override
				public void accept(Void aVoid) {
					Broadcaster.super.onComplete();
				}
			}, null);
		} else {
			super.onComplete();
		}
	}

	@Override
	protected PushSubscription<O> createSubscription(Subscriber<? super O> subscriber, CompletableQueue<O> queue) {
		if (queue != null) {
			return new ReactiveSubscription<O>(this, subscriber, queue) {

				@Override
				protected void onRequest(long elements) {
					if (upstreamSubscription != null) {
						super.onRequest(elements);
						requestUpstream(capacity, buffer.isComplete(), elements);
					}
				}
			};
		} else {
			return super.createSubscription(subscriber, null);
		}
	}

	@Override
	protected PushSubscription<O> createSubscription(Subscriber<? super O> subscriber, boolean reactivePull) {
		if (reactivePull) {
			return super.createSubscription(subscriber, true);
		} else {
			return super.createSubscription(subscriber,
					dispatcher != SynchronousDispatcher.INSTANCE &&
							(upstreamSubscription != null && !upstreamSubscription.hasPublisher()));
		}
	}

	@Override
	protected void doStart(long pending) {
		//
	}

	@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		if(upstreamSubscription != null && !terminated) {
			requestMore(elements);
		} else {
			if(PENDING_UPDATER.addAndGet(this, elements) < 0l) pendingRequests = Long.MAX_VALUE;
		}
	}

	@Override
	public void requestMore(long n) {
		Action.checkRequest(n);
		long toRequest = n != Long.MAX_VALUE ? Math.min(capacity, n) : Long.MAX_VALUE;
		PushSubscription<O> upstreamSubscription = this.upstreamSubscription;

		if (upstreamSubscription != null) {
			toRequest = toRequest - Math.max(upstreamSubscription.pendingRequestSignals(), 0l);
			toRequest = toRequest < 0l ? 0l : toRequest;

			if (n == Long.MAX_VALUE || PENDING_UPDATER.addAndGet(this, n - toRequest) < 0l) {
				PENDING_UPDATER.set(this, Long.MAX_VALUE);
			}

			if (toRequest > 0) {
					upstreamSubscription.accept(toRequest);
			}
		}else if (n == Long.MAX_VALUE || PENDING_UPDATER.addAndGet(this, n - toRequest) < 0l) {
			PENDING_UPDATER.set(this, Long.MAX_VALUE);
		}
	}

	@Override
	public Broadcaster<O> capacity(long elements) {
		super.capacity(elements);
		return this;
	}

	@Override
	public String toString() {
		return super.toString() + "{overflow=" + pendingRequests + "}";
	}
}
