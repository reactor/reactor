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

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Processors;
import reactor.Timers;
import reactor.core.error.CancelException;
import reactor.core.error.InsufficientCapacityException;
import reactor.core.processor.ProcessorGroup;
import reactor.fn.timer.Timer;
import reactor.rx.action.ProcessorAction;
import reactor.rx.subscription.SwapSubscription;

/**
 * A {@code Broadcaster} is a subclass of {@code Stream} which exposes methods for publishing values into the pipeline.
 * It is possible to publish discreet values typed to the generic type of the {@code Stream} as well as error conditions
 * and the Reactive Streams "complete" signal via the {@link #onComplete()} method.
 * @author Stephane Maldini
 */
public class Broadcaster<O> extends ProcessorAction<O, O> {

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link reactor.rx.action
	 * .Broadcaster#onNext(Object)}, {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}. Values
	 * broadcasted are directly consumable by subscribing to the returned instance.
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link reactor.rx.broadcast.Broadcaster}
	 */
	public static <T> Broadcaster<T> create() {
		return create(null, false);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link Broadcaster#onNext(Object)}, {@link
	 * Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}. Values broadcasted are directly consumable by
	 * subscribing to the returned instance.
	 * @param timer the Reactor {@link reactor.fn.timer.Timer} to use downstream
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> create(Timer timer) {
		return create(timer, false);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link Broadcaster#onNext(Object)}, {@link
	 * Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}. Values broadcasted are directly consumable by
	 * subscribing to the returned instance.
	 * @param timer the Reactor {@link reactor.fn.timer.Timer} to use downstream
	 * @param autoCancel Propagate cancel upstream
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> create(Timer timer, boolean autoCancel) {
		return from(Processors.<T>emitter(autoCancel), timer, autoCancel);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link Broadcaster#onNext(Object)}, {@link
	 * Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}. Values broadcasted are directly consumable by
	 * subscribing to the returned instance. <p> Will not bubble up  any {@link CancelException}
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> passthrough() {
		return passthrough(null);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link Broadcaster#onNext(Object)}, {@link
	 * Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}. Values broadcasted are directly consumable by
	 * subscribing to the returned instance. <p> Will not bubble up  any {@link CancelException}
	 * @param timer the Reactor {@link reactor.fn.timer.Timer} to use downstream
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> passthrough(Timer timer) {
		return from(ProcessorGroup.sync(), timer, true);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link Broadcaster#onNext(Object)}, {@link
	 * Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}. Values broadcasted are directly consumable by
	 * subscribing to the returned instance.
	 * @param emitter Identity processor to support broadcasting
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> from(Processor<T, T> emitter) {
		return from(emitter, false);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link Broadcaster#onNext(Object)}, {@link
	 * Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}. Values broadcasted are directly consumable by
	 * subscribing to the returned instance.
	 * @param emitter Identity processor to support broadcasting
	 * @param autoCancel Propagate cancel upstream
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> from(Processor<T, T> emitter, boolean autoCancel) {
		return from(emitter, null, autoCancel);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link Broadcaster#onNext(Object)}, {@link
	 * Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}. Values broadcasted are directly consumable by
	 * subscribing to the returned instance.
	 * @param timer the Reactor {@link reactor.fn.timer.Timer} to use downstream
	 * @param autoCancel Propagate cancel upstream
	 * @param emitter Identity processor to support broadcasting
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> from(Processor<T, T> emitter, Timer timer, boolean autoCancel) {
		return new Broadcaster<T>(emitter, timer, autoCancel);
	}


	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link Broadcaster#onNext(Object)}, {@link
	 * Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}. Values broadcasted are directly consumable by
	 * subscribing to the returned instance.
	 * @param emitter Identity processor to support broadcasting
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> from(ProcessorGroup emitter) {
		return from(emitter, false);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link Broadcaster#onNext(Object)}, {@link
	 * Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}. Values broadcasted are directly consumable by
	 * subscribing to the returned instance.
	 * @param emitter Identity processor to support broadcasting
	 * @param autoCancel Propagate cancel upstream
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> from(ProcessorGroup emitter, boolean autoCancel) {
		return from(emitter, null, autoCancel);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link Broadcaster#onNext(Object)}, {@link
	 * Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}. Values broadcasted are directly consumable by
	 * subscribing to the returned instance.
	 * @param timer the Reactor {@link reactor.fn.timer.Timer} to use downstream
	 * @param autoCancel Propagate cancel upstream
	 * @param emitter Identity processor to support broadcasting
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Broadcaster<T> from(ProcessorGroup emitter, Timer timer, boolean autoCancel) {
		return from(emitter.dispatchOn(), timer, autoCancel);
	}

	/**
	 * INTERNAL
	 */

	private final Timer               timer;
	private final boolean             ignoreDropped;
	private final SwapSubscription<O> subscription;

	protected Broadcaster(Processor<O, O> processor, Timer timer, boolean ignoreDropped) {
		this(processor, processor, timer, ignoreDropped);
	}

	protected Broadcaster(
			Subscriber<O> receiver,
			Publisher<O> publisher,
			Timer timer,
			boolean ignoreDropped) {
		super(receiver, publisher);
		this.timer = timer;
		this.ignoreDropped = ignoreDropped;
		this.subscription = SwapSubscription.create();

		receiver.onSubscribe(subscription);
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		this.subscription.swapTo(subscription);
	}

	@Override
	public void onNext(O ev) {
		try {
			if(subscription.isCancelled()){
				throw CancelException.INSTANCE;
			}
			receiver.onNext(ev);
		}
		catch (InsufficientCapacityException | CancelException c) {
			if (!ignoreDropped) {
				throw c;
			}
		}
	}

	@Override
	public void onError(Throwable t) {
		try {
			receiver.onError(t);
		}
		catch (InsufficientCapacityException | CancelException c) {
			//IGNORE
		}
	}

	@Override
	public void onComplete() {
		try {
			receiver.onComplete();
		}
		catch (InsufficientCapacityException | CancelException c) {
			//IGNORE
		}
	}

	@Override
	public Timer getTimer() {
		return timer != null ? timer : Timers.globalOrNull();
	}
}
