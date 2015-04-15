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
import org.reactivestreams.Subscription;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.queue.CompletableQueue;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
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

	@SuppressWarnings("unchecked")
	static public final Subscription HOT_SUBSCRIPTION = new PushSubscription(null, null) {
		@Override
		public void request(long n) {
			//IGNORE
		}

		@Override
		public void cancel() {
			//IGNORE
		}
	};

	protected final Dispatcher  dispatcher;
	protected final Environment environment;

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
	 *
	 * INTERNAL
	 */
	@SuppressWarnings("unchecked")
	protected Broadcaster(Environment environment, Dispatcher dispatcher, long capacity) {
		super(capacity);
		this.dispatcher = dispatcher;
		this.environment = environment;

		//start broadcaster
		this.upstreamSubscription = (PushSubscription<O>)HOT_SUBSCRIPTION;
	}

	@Override
	public final Dispatcher getDispatcher() {
		return dispatcher;
	}

	@Override
	protected void doNext(O ev) {
		broadcastNext(ev);
	}

	@Override
	public void onNext(O ev) {
		if(ev == null){
			throw new NullPointerException("Spec 2.13: Signal cannot be null");
		}
		if (!dispatcher.inContext()) {
			dispatcher.dispatch(ev, this, null);
		} else {
			super.onNext(ev);
		}
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		if(upstreamSubscription == HOT_SUBSCRIPTION){
			upstreamSubscription = null;
			super.onSubscribe(subscription);

			PushSubscription<O> downSub = downstreamSubscription;
			if(downSub != null && downSub.pendingRequestSignals() > 0L ){
				subscription.request(downSub.pendingRequestSignals());
			}

		}else{
			super.onSubscribe(subscription);
		}
	}

	@Override
	public void onError(Throwable cause) {
		if(cause == null){
			throw new NullPointerException("Spec 2.13: Signal cannot be null");
		}
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
	protected void subscribeWithSubscription(Subscriber<? super O> subscriber, PushSubscription<O> subscription) {
		try {
			if (!addSubscription(subscription)) {
				subscriber.onError(new IllegalStateException("The subscription cannot be linked to this Stream"));
			} else {
				subscriber.onSubscribe(subscription);
			}
		} catch (Exception e) {
			subscriber.onError(e);
		}
	}

	@Override
	public void cancel() {
		if(upstreamSubscription != HOT_SUBSCRIPTION){
			super.cancel();
		}
	}

	@Override
	public void recycle() {
		if(HOT_SUBSCRIPTION != upstreamSubscription){
			upstreamSubscription = null;
		} else {
			downstreamSubscription = null;
		}

	}

	@Override
	public Broadcaster<O> capacity(long elements) {
		super.capacity(elements);
		return this;
	}

	@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		if (upstreamSubscription != null && upstreamSubscription != HOT_SUBSCRIPTION && !terminated) {
			requestMore(elements);
		} else {
			PushSubscription<O> _downstreamSubscription = downstreamSubscription;
			if (_downstreamSubscription != null && _downstreamSubscription.pendingRequestSignals() == 0L) {
				_downstreamSubscription.updatePendingRequests(elements);
			}
		}
	}


	/*@Override
	protected void requestUpstream(long capacity, boolean terminated, long elements) {
		requestMore(elements);
	}

	@Override
	public void requestMore(long n) {
		Action.checkRequest(n);
		PushSubscription<O> upstreamSubscription = this.upstreamSubscription;
		if(upstreamSubscription != null) {
			dispatcher.dispatch(n, upstreamSubscription, null);
		}
	}*/


}
