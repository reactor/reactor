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
import reactor.core.error.Exceptions;
import reactor.core.error.InsufficientCapacityException;
import reactor.core.processor.ProcessorGroup;
import reactor.core.timer.Timer;
import reactor.rx.subscriber.SerializedSubscriber;
import reactor.rx.subscription.SwapSubscription;

/**
 * Broadcasters are akin to Reactive Extensions Subject. Extending Stream, they fulfil the
 * {@link org.reactivestreams.Processor} contract.
 *
 * Some broadcasters might be shared and will require serialization as onXXXX handle should not be invoke concurrently.
 * {@link #serialize} can take care of this specific issue.
 *
 * @author Stephane Maldini
 */
public class Broadcaster<O> extends StreamProcessor<O, O> {

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
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link reactor.rx.action
	 * .Broadcaster#onNext(Object)}, {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}. Values
	 * broadcasted are directly consumable by subscribing to the returned instance.
	 * @param autoCancel Propagate cancel upstream
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link reactor.rx.broadcast.Broadcaster}
	 */
	public static <T> Broadcaster<T> create(boolean autoCancel) {
		return create(null, autoCancel);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link Broadcaster#onNext(Object)}, {@link
	 * Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}. Values broadcasted are directly consumable by
	 * subscribing to the returned instance.
	 * @param timer the Reactor {@link Timer} to use downstream
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
	 * @param timer the Reactor {@link Timer} to use downstream
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
	 * @param timer the Reactor {@link Timer} to use downstream
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> passthrough(Timer timer) {
		return from(ProcessorGroup.sync(), timer, true);
	}

	/**
	 * Build a {@literal Broadcaster} that will support concurrent signals (onNext, onError, onComplete) and use
	 * thread-stealing to serialize underlying emitter processor calls.
	 *
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> serialize() {
		return serialize(null);
	}

	/**
	 * Build a {@literal Broadcaster} that will support concurrent signals (onNext, onError, onComplete) and use
	 * thread-stealing to serialize underlying emitter processor calls.
	 *
	 * @param timer the Reactor {@link Timer} to use downstream
	 * @param <T> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> serialize(Timer timer) {
		Processor<T, T> processor = Processors.emitter();
		return new Broadcaster<T>(SerializedSubscriber.create(processor), processor, timer, true);
	}

	/**
	 * Build a {@literal Broadcaster}, ready to broadcast values with {@link Broadcaster#onNext(Object)}, {@link
	 * Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}. Values broadcasted are directly consumable by
	 * subscribing to the returned instance.
	 * @param emitter Identity processor to support broadcasting
	 * @param <I> the type of values passing through the {@literal Broadcaster}
	 * @param <O> the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	@SuppressWarnings("unchecked")
	public static <I, O> StreamProcessor<I, O> from(Processor<I, O> emitter) {
		return (StreamProcessor<I, O>)from((Processor<O, O>)emitter, false);
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
	 * @param timer the Reactor {@link Timer} to use downstream
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
	 * @param timer the Reactor {@link Timer} to use downstream
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
	 * Build a {@literal Broadcaster}, rfirst broadcasting the most recent signal then starting with the passed value,
	 * then ready to broadcast values with {@link reactor.rx.action
	 * .Broadcaster#onNext(Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete
	 * ()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 * <p>
	 * A serialized broadcaster will make sure that even in a multhithreaded scenario, only one thread will be able to
	 * broadcast at a time.
	 * The synchronization is non blocking for the publisher, using thread-stealing and first-in-first-served patterns.
	 *
	 * @param value the value to start with the sequence
	 * @param <T> the type of values passing through the {@literal action}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> replayLastOrDefault(T value) {
		return replayLastOrDefault(value, null);
	}

	/**
	 * Build a {@literal Broadcaster}, first broadcasting the most recent signal then ready to broadcast values with
	 * {@link #onNext(Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 *
	 * @param timer the {@link Timer} to use downstream
	 * @param <T>        the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> replayLast(Timer timer) {
		return replayLastOrDefault(null, timer);
	}

	/**
	 * Build a {@literal Broadcaster}, first broadcasting the most recent signal then starting with the passed value,
	 * then  ready to broadcast values with {@link #onNext(Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 *
	 * @param value the value to start with the sequence
	 * @param timer the {@link Timer} to use downstream
	 * @param <T>        the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> replayLastOrDefault(T value, Timer timer) {
		Broadcaster<T> b = new Broadcaster<T>(Processors.<T>replay(1), timer, false);
		if(value != null){
			b.onNext(value);
		}
		return b;
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
				Exceptions.onNextDropped(ev);
			}
			subscription.ack();
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
