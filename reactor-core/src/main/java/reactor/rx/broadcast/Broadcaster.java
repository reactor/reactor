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
	private boolean keepAlive = false;

	public Broadcaster(Dispatcher dispatcher, long capacity) {
		super(dispatcher, capacity);
	}

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
		return Action.<T>passthrough(SynchronousDispatcher.INSTANCE).keepAlive();
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values, ready to broadcast values with {@link
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
	 * Build a {@literal Broadcaster}, ready to broadcast values, ready to broadcast values with {@link
	 * reactor.rx.action.Action#onNext(Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 *
	 * @param dispatcher the {@link reactor.core.Dispatcher} to use
	 * @param <T> the type of values passing through the {@literal Broadcaster}
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
				" For concurrent consume, refer to Stream#partition/groupBy() method and assign individual single dispatchers");
		Broadcaster<T> broadcaster = Action.<T>passthrough(dispatcher);
		broadcaster.env(env).capacity(dispatcher.backlogSize() > 0 ?
				(RESERVED_SLOTS > dispatcher.backlogSize() ?
						dispatcher.backlogSize() :
						dispatcher.backlogSize() - RESERVED_SLOTS) :
				Long.MAX_VALUE);
		return broadcaster.keepAlive();
	}

	@Override
	protected void doNext(O ev) {
		broadcastNext(ev);
	}


	@Override
	protected void doComplete() {
		if (!keepAlive && downstreamSubscription == null) {
			cancel();
		}
		broadcastComplete();
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
	public Broadcaster<O> env(Environment environment) {
		super.env(environment);
		return this;
	}

	@Override
	public Broadcaster<O> capacity(long elements) {
		super.capacity(elements);
		return this;
	}

	public Broadcaster<O> keepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
		return this;
	}

	@Override
	public Broadcaster<O> keepAlive() {
		this.keepAlive(true);
		return this;
	}

	@Override
	protected void onShutdown() {
		if (!keepAlive) {
			super.onShutdown();
		}
	}

	@Override
	public String toString() {
		return super.toString() + "{keepAlive=" + keepAlive +
				"}";
	}
}
