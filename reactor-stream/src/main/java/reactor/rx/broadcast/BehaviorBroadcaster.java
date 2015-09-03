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
import reactor.Timers;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.fn.timer.Timer;
import reactor.rx.action.Signal;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.ReactiveSubscription;

import java.util.Queue;

/**
 * A {@code Broadcaster} is a subclass of {@code Stream} which exposes methods for publishing values into the pipeline.
 * It is possible to publish discreet values typed to the generic type of the {@code Stream} as well as error conditions
 * and the Reactive Streams "complete" signal via the {@link #onComplete()} method.
 *
 * @author Stephane Maldini
 */
public final class BehaviorBroadcaster<O> extends Broadcaster<O> {

	/**
	 * Build a {@literal Broadcaster}, rfirst broadcasting the most recent signal then starting with the passed value,
	 * then ready to broadcast values with {@link reactor.rx.action
	 * .Broadcaster#onNext(Object)},
	 * {@link reactor.rx.broadcast.Broadcaster#onError(Throwable)}, {@link reactor.rx.broadcast.Broadcaster#onComplete
	 * ()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 * <p>
	 * A serialized broadcaster will make sure that even in a multhithreaded scenario, only one thread will be able to
	 * broadcast at a time.
	 * The synchronization is non blocking for the publisher, using thread-stealing and first-in-first-served patterns.
	 *
	 * @param value the value to start with the sequence
	 * @param <T> the type of values passing through the {@literal action}
	 * @return a new {@link reactor.rx.action.Action}
	 */
	public static <T> Broadcaster<T> first(T value) {
		return new BehaviorBroadcaster<>(null, value);
	}

	/**
	 * Build a {@literal Broadcaster}, first broadcasting the most recent signal then starting with the passed value,
	 * then  ready to broadcast values with {@link
	 * reactor.rx.action.Action#onNext(Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 *
	 * @param value the value to start with the sequence
	 * @param timer the {@link Timer} to use downstream
	 * @param <T>        the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> first(T value, Timer timer) {
		return new BehaviorBroadcaster<>(timer, value);
	}

	/**
	 * Build a {@literal Broadcaster}, first broadcasting the most recent signal then ready to broadcast values with
	 * {@link
	 * reactor.rx.action.Action#onNext(Object)},
	 * {@link Broadcaster#onError(Throwable)}, {@link Broadcaster#onComplete()}.
	 * Values broadcasted are directly consumable by subscribing to the returned instance.
	 *
	 * @param timer the {@link Timer} to use downstream
	 * @param <T>        the type of values passing through the {@literal Broadcaster}
	 * @return a new {@link Broadcaster}
	 */
	public static <T> Broadcaster<T> create(Timer timer) {
		return first(null, timer);
	}

	/**
	 * INTERNAL
	 */

	private final BufferedSignal<O> lastSignal = new BufferedSignal<O>(null);

	private BehaviorBroadcaster(Timer timer, O defaultVal) {
		super(timer, false);
		if (defaultVal != null) {
			lastSignal.type = Signal.Type.NEXT;
			lastSignal.value = defaultVal;
		}
	}

	private final static class BufferedSignal<O> {
		O           value;
		Throwable   error;
		Signal.Type type;

		public BufferedSignal(Signal.Type type) {
			this.type = type;
		}
	}

	@Override
	public void onNext(O ev) {
		synchronized (this) {
			if (lastSignal.type == Signal.Type.COMPLETE ||
			  lastSignal.type == Signal.Type.ERROR)
				return;
			lastSignal.value = ev;
			lastSignal.error = null;
			lastSignal.type = Signal.Type.NEXT;
		}
		try {
			broadcastNext(ev);
		} catch (CancelException ce) {
			//IGNORE since cached
		}
	}



	@Override
	protected void doComplete() {
		synchronized (this) {
			if (lastSignal.type == Signal.Type.COMPLETE ||
			  lastSignal.type == Signal.Type.ERROR)
				return;
			lastSignal.error = null;
			lastSignal.type = Signal.Type.COMPLETE;
		}
		super.doComplete();
	}

	@Override
	protected void doError(Throwable ev) {
		synchronized (this) {
			if (lastSignal.type == Signal.Type.COMPLETE ||
			  lastSignal.type == Signal.Type.ERROR)
				return;
			lastSignal.value = null;
			lastSignal.error = ev;
			lastSignal.type = Signal.Type.ERROR;
		}
		if (downstreamSubscription != null) {
			try {
				downstreamSubscription.onError(ev);
			} catch (Throwable t) {
				Exceptions.throwIfFatal(t);
				throw ReactorFatalException.create(t);
			}
		}
	}

	@Override
	protected PushSubscription<O> createSubscription(final Subscriber<? super O> subscriber, Queue<O>
	  queue) {
		final BufferedSignal<O> withDefault;
		synchronized (this) {
			if (lastSignal.type != null) {
				withDefault = new BufferedSignal<>(lastSignal.type);
				withDefault.error = lastSignal.error;
				withDefault.value = lastSignal.value;
				withDefault.type = lastSignal.type;
			} else {
				withDefault = null;
			}
		}

		if (withDefault != null) {
			if (withDefault.type == Signal.Type.COMPLETE) {
				return new PushSubscription<O>(this, subscriber) {
					@Override
					public void request(long n) {
						//Promise behavior, emit last value before completing
						if (n > 0 && capacity == 1l && withDefault.value != null) {
							capacity = 0l;
							subscriber.onNext(withDefault.value);
						}
						onComplete();
					}
				};
			} else if (withDefault.type == Signal.Type.ERROR) {
				return new PushSubscription<O>(this, subscriber) {
					@Override
					public void request(long n) {
						onError(withDefault.error);
					}
				};
			} else {
				if (queue != null) {
					queue.add(withDefault.value);
					return new ReactiveSubscription<O>(this, subscriber, queue) {

						@Override
						protected void onRequest(long elements) {
							if (upstreamSubscription != null) {
								super.onRequest(elements);
								requestUpstream(capacity, terminalSignalled, elements);
							}
						}
					};
				} else {
					return new PushSubscription<O>(this, subscriber) {
						boolean started = false;

						@Override
						public void request(long n) {
							if (!started && n > 0) {
								started = true;
								subscriber.onNext(withDefault.value);
								if (n - 1 > 0) {
									super.request(n - 1);
								}
							} else {
								super.request(n);
							}
						}

						@Override
						protected void onRequest(long elements) {
							if (upstreamSubscription == null) {
								updatePendingRequests(elements);
							} else {
								requestUpstream(NO_CAPACITY, isComplete(), elements);
							}
						}
					};
				}
			}
		} else {
			return super.createSubscription(subscriber, queue);
		}
	}
}
