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
package reactor.rx.action;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.function.Consumer;
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;
import reactor.rx.StreamUtils;
import reactor.timer.Timer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 * @since 1.1
 */
public class Action<I, O> extends Stream<O> implements Processor<I, O>, Consumer<I> {

	//private static final Logger log = LoggerFactory.getLogger(Action.class);

	/**
	 * onComplete, onError, request, onSubscribe are dispatched events, therefore up to capacity + 4 events can be
	 * in-flight
	 * stacking into a Dispatcher.
	 */
	public static final int RESERVED_SLOTS = 4;

	protected int     pendingNextSignals = 0;
	protected int     currentNextSignals = 0;
	protected boolean firehose           = false;

	protected final Consumer<Integer> requestConsumer = new Consumer<Integer>() {
		@Override
		public void accept(Integer n) {
			try {
				if (firehose) {
					subscription.request(n);
					return;
				}

				int previous = pendingNextSignals;
				if ((pendingNextSignals += n) < 0) pendingNextSignals = Integer.MAX_VALUE;

				if (previous < batchSize) {
					int upperBound = batchSize - previous;
					int toRequest = n - previous;
					subscription.request(toRequest > upperBound ? upperBound : n);
				}

			} catch (Throwable t) {
				doError(t);
			}
		}
	};

	protected Subscription subscription;

	public static <O> Action<O, O> passthrough(Dispatcher dispatcher) {
		return new Action<O, O>(dispatcher) {
			@Override
			protected void doNext(O ev) {
				broadcastNext(ev);
			}
		};
	}

	public Action() {
		super();
	}

	public Action(Dispatcher dispatcher) {
		super(dispatcher);
	}

	public Action(Dispatcher dispatcher, int batchSize) {
		super(dispatcher, batchSize);
	}

	public void available() {
		if (subscription != null && !pause) {
			dispatch(batchSize, requestConsumer);
		}
	}

	protected void requestUpstream(AtomicLong capacity, boolean terminated, int elements) {
		if (subscription != null && !terminated) {
			int currentCapacity = capacity.intValue();
			currentCapacity = currentCapacity == -1 ? elements : currentCapacity;
			if (!pause && (currentCapacity > 0)) {
				final int remaining = currentCapacity > elements ? elements : currentCapacity;
				onRequest(remaining);
			}
		}
	}

	@Override
	protected StreamSubscription<O> createSubscription(final Subscriber<O> subscriber) {
		if (subscription == null) {
			return new StreamSubscription<O>(this, subscriber) {
				@Override
				public void request(int elements) {
					super.request(elements);
					requestUpstream(capacity, buffer.isComplete(), elements);
				}
			};
		} else {
			return new StreamSubscription.Firehose<O>(this, subscriber) {
				@Override
				public void request(int elements) {
					requestUpstream(capacity, isComplete(), elements);
				}
			};
		}
	}


	@Override
	public void accept(I i) {
		try {
			++currentNextSignals;
			doNext(i);
			if (!firehose) {
				doPendingRequest();
			}
		} catch (Throwable cause) {
			doError(cause);
		}
	}

	protected void doPendingRequest() {
		if (currentNextSignals == batchSize) {
			int toRequest = batchSize > pendingNextSignals ? pendingNextSignals : batchSize;
			currentNextSignals = 0;

			if (toRequest > 0) {
				pendingNextSignals -= toRequest;
				subscription.request(toRequest);
			}
		}
	}

	protected <E> void trySyncDispatch(E data, Consumer<E> action) {
		if (firehose) {
			action.accept(data);
		} else {
			dispatch(data, action);
		}
	}

	@Override
	public void onNext(I ev) {
		trySyncDispatch(ev, this);
	}

	protected void onRequest(final int n) {
		trySyncDispatch(n, requestConsumer);
	}

	@Override
	public void onComplete() {
		trySyncDispatch(null, new Consumer<Void>() {
			@Override
			public void accept(Void any) {
				try {
					doComplete();
					/*if(!keepAlive){
						cancel();
					}*/
				} catch (Throwable t) {
					doError(t);
				}
			}
		});

	}

	@Override
	public void onError(Throwable cause) {
		try {
			trySyncDispatch(cause, new Consumer<Throwable>() {
				@Override
				public void accept(Throwable throwable) {
					doError(throwable);
				}
			});
		} catch (Throwable dispatchError) {
			error = dispatchError;
		}
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		if (this.subscription == null) {
			this.subscription = subscription;
			this.state = State.READY;
			this.firehose = StreamSubscription.Firehose.class.isAssignableFrom(subscription.getClass());

			final CountDownLatch startedCountDown = new CountDownLatch(1);

			dispatch(subscription, new Consumer<Subscription>() {
				@Override
				public void accept(Subscription subscription) {
					try {
						doSubscribe(subscription);
						if (!firehose) {
							startedCountDown.countDown();
						}
					} catch (Throwable t) {
						doError(t);
					}
				}
			});
			if (!firehose) {
				try {
					startedCountDown.await();
				} catch (InterruptedException e) {
					doError(e);
				}
			}
		} else {
			throw new IllegalStateException("Already subscribed");
		}
	}

	@Override
	protected void removeSubscription(StreamSubscription<O> sub) {
		super.removeSubscription(sub);
		if (getState() == State.SHUTDOWN && subscription != null) {
			subscription.cancel();
		}
	}

	@Override
	public Action<I, O> cancel() {
		if (subscription != null) {
			subscription.cancel();
			subscription = null;
		}
		super.cancel();
		return this;
	}

	@Override
	public Action<I, O> pause() {
		super.pause();
		return this;
	}

	@Override
	public Action<I, O> resume() {
		super.resume();
		if(subscription != null){

			dispatch(new Consumer<Void>() {
				@Override
				public void accept(Void integer) {
					if(pendingNextSignals > 0){
						int toRequest = pendingNextSignals;
						pendingNextSignals = 0;
						requestConsumer.accept(toRequest);
					}
				}
			});
		}
		return this;
	}

	@Override
	public Action<O, O> throttle(long period, long delay, Timer timer) {
		final ThrottleAction<O> d = new ThrottleAction<O>(
				getDispatcher(),
				timer,
				period,
				delay
		);
		d.env(environment).capacity(batchSize).setKeepAlive(keepAlive);
		checkAndSubscribe(d, new StreamSubscription<O>(this, d) {
			@Override
			public void request(int elements) {
				if (capacity.get() == 0) {
					super.request(1);
				}
				requestUpstream(new AtomicLong(elements), buffer.isComplete(), elements);
			}
		});
		return d;
	}

	@Override
	public String debug() {
		return StreamUtils.browse(findOldestStream(false));
	}

	public <E> CombineAction<E, O, Action<I, O>> combine() {
		return combine(false);
	}

	@SuppressWarnings("unchecked")
	public <E> CombineAction<E, O, Action<I, O>> combine(boolean reuse) {
		final Action<E, O> subscriber = (Action<E, O>) findOldestStream(reuse);
		subscriber.subscription = null;
		return new CombineAction<E, O, Action<I, O>>(this, subscriber);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Action<I, O> capacity(int elements) {
		return (Action<I, O>) super.capacity(elements);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Action<I, O> env(Environment environment) {
		return (Action<I, O>) super.env(environment);
	}

	public Action<?, ?> findOldestStream(boolean resetState) {
		Action<?, ?> that = this;

		if (resetState) {
			resetState(that);
		}
		while (that.subscription != null
				&& StreamSubscription.class.isAssignableFrom(that.subscription.getClass())
				&& Action.class.isAssignableFrom(((StreamSubscription<?>) that.subscription).getPublisher().getClass())
				) {

			that = (Action<?, ?>) ((StreamSubscription<?>) that.subscription).getPublisher();

			if (resetState) {
				resetState(that);
			}

		}
		return that;
	}

	protected void doSubscribe(Subscription subscription) {
	}

	protected void doComplete() {
		broadcastComplete();
	}

	protected void doNext(I ev) {
	}

	protected void doError(Throwable ev) {
		broadcastError(ev);
	}

	protected void resetState(Action<?, ?> action) {
		action.state = State.READY;
		action.error = null;
	}

	public Subscription getSubscription() {
		return subscription;
	}

	@Override
	@SuppressWarnings("unchecked")
	public String toString() {
		return "{" +
				"dispatcher=" + dispatcher.getClass().getSimpleName().replaceAll("Dispatcher", "") +
				((!SynchronousDispatcher.class.isAssignableFrom(dispatcher.getClass()) ? (":" + dispatcher.remainingSlots()) :
						"")) +
				", state=" + getState() +
				", max-capacity=" + getMaxCapacity() +
				(subscription != null &&
						StreamSubscription.class.isAssignableFrom(subscription.getClass()) ?
						", subscription=" + subscription +
								", pending=" + pendingNextSignals +
								", currentNextSignals=" + currentNextSignals
						: (subscription != null ? ", subscription=" + subscription : "")
				) + '}';
	}

}
