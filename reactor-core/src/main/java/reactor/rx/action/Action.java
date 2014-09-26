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
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.function.Consumer;
import reactor.rx.Stream;
import reactor.rx.StreamSubscription;
import reactor.rx.StreamUtils;
import reactor.rx.action.support.NonBlocking;
import reactor.rx.action.support.SpecificationExceptions;
import reactor.timer.Timer;

import java.util.concurrent.atomic.AtomicLong;

/**
 * An Action is a reactive component to subscribe to a {@link org.reactivestreams.Publisher} and in particular
 * to a {@link reactor.rx.Stream}. Stream is usually the place where actions are created.
 * <p>
 * An Action is also a data producer, and therefore implements {@link org.reactivestreams.Processor}.
 * An imperative programming equivalent of an action is a method or function. The main difference is that it also
 * reacts on various {@link org.reactivestreams.Subscriber} signals and produce an output data {@param O} for
 * any downstream subscription.
 * <p>
 * The implementation specifics of an Action lies in two core features:
 * - Its signal scheduler on {@link reactor.event.dispatch.Dispatcher}
 * - Its smart capacity awareness to prevent {@link reactor.event.dispatch.Dispatcher} overflow
 * <p>
 * In effect, an Action will take care of concurrent notifications through its single threaded Dispatcher.
 * Up to a maximum capacity defined with {@link this#capacity(long)} will be allowed to be dispatched by requesting
 * the tracked remaining slots to the upstream {@link org.reactivestreams.Subscription}. This maximum in-flight data
 * is a value to tune accordingly with the system and the requirements. An Action will bypass this feature anytime it is
 * not the root of stream processing chain e.g.:
 * <p>
 * stream.filter(..).map(..) :
 * <p>
 * In that Stream, filter is a FilterAction and has no upstream action, only the publisher it is attached to.
 * The FilterAction will decide to be capacity aware and will track demand.
 * The MapAction will however behave like a firehose and will not track the demand, passing any request upstream.
 * <p>
 * Implementing an Action is highly recommended to work with Stream without dealing with tracking issues and other
 * threading matters. Usually an implementation will override any doXXXXX method where 'do' is an hint that logic will
 * safely be dispatched to avoid race-conditions.
 *
 * @param <I> The input {@link this#onNext(Object)} signal
 * @param <O> The output type to listen for with {@link this#subscribe(org.reactivestreams.Subscriber)}
 * @author Stephane Maldini
 * @since 1.1, 2.0
 */
public abstract class Action<I, O> extends Stream<O> implements Processor<I, O>, Consumer<I>, NonBlocking {

	//private static final Logger log = LoggerFactory.getLogger(Action.class);

	/**
	 * onComplete, onError, request, onSubscribe are dispatched events, therefore up to capacity + 4 events can be
	 * in-flight
	 * stacking into a Dispatcher.
	 */
	public static final int RESERVED_SLOTS = 4;

	protected long    pendingNextSignals = 0;
	protected long    currentNextSignals = 0;
	protected boolean firehose           = false;
	protected Subscription subscription;

	protected final Consumer<Long> requestConsumer = new Consumer<Long>() {
		@Override
		public void accept(Long n) {
			try {
				if (subscription == null) {
					if ((pendingNextSignals += n) < 0)
						doError(SpecificationExceptions.spec_3_17_exception(pendingNextSignals, n));
					return;
				}

				if (firehose) {
					currentNextSignals = 0;
					subscription.request(n);
					return;
				}

				long previous = pendingNextSignals;
				pendingNextSignals += n;

				if (previous < capacity) {
					long toRequest = n + previous;
					toRequest = Math.min(toRequest, capacity);
					pendingNextSignals -= toRequest;
					currentNextSignals = 0;
					subscription.request(toRequest);
				}

			} catch (Throwable t) {
				doError(t);
			}
		}
	};

	public static void checkRequest(long n) {
		if (n <= 0l) {
			throw SpecificationExceptions.spec_3_09_exception(n);
		}
	}

	/**
	 * A simple NOOP action that can be used to isolate Stream properties such as dispatcher or capacity.
	 *
	 * @param dispatcher
	 * @param <O>
	 * @return a new Action subscribed to this one
	 */
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

	public Action(Dispatcher dispatcher, long batchSize) {
		super(dispatcher, batchSize);
	}

	public void available() {
		if (subscription != null && !pause) {
			dispatch(capacity, requestConsumer);
		}
	}

	@Override
	public void subscribe(Subscriber<? super O> subscriber) {
		checkAndSubscribe(subscriber, createSubscription(subscriber,
				subscription == null || !NonBlocking.class.isAssignableFrom(subscriber.getClass()))
		);
	}

	@Override
	public void accept(I i) {
		try {
			++currentNextSignals;
			doNext(i);
			if (!firehose && currentNextSignals == capacity) {
				doPendingRequest();
			}
		} catch (Throwable cause) {
			doError(cause);
		}
	}


	@Override
	public void onNext(I ev) {
		trySyncDispatch(ev, this);
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
		if (this.subscription != null) {
			subscription.cancel();
			return;
		}

		this.subscription = subscription;
		this.state = State.READY;
		this.firehose = StreamSubscription.Firehose.class.isAssignableFrom(subscription.getClass());


		dispatch(subscription, new Consumer<Subscription>() {
			@Override
			public void accept(Subscription subscription) {
				try {
					doSubscribe(subscription);
				} catch (Throwable t) {
					doError(t);
				}
			}
		});
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
		if (subscription != null) {

			trySyncDispatch(null, new Consumer<Void>() {
				@Override
				public void accept(Void integer) {
					long toRequest = generateDemandFromPendingRequests();
					if (toRequest > 0) {
						pendingNextSignals -= toRequest;
						requestConsumer.accept(toRequest);
					}
				}
			});
		}
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public StreamUtils.StreamVisitor debug() {
		return StreamUtils.browse(findOldestUpstream(Stream.class, false));
	}

	/**
	 * Combine the most ancient upstream action to act as the {@link org.reactivestreams.Subscriber} input component and
	 * the current action to act as the {@link org.reactivestreams.Publisher}.
	 * <p>
	 * Useful to share and ship a full stream whilst hiding the staging actions in the middle
	 *
	 * @param <E>
	 * @return new Action
	 */
	public <E> CombineAction<E, O> combine() {
		return combine(false);
	}

	/**
	 * Combine the most ancient upstream action to act as the {@link org.reactivestreams.Subscriber} input component and
	 * the current action to act as the {@link org.reactivestreams.Publisher}.
	 * <p>
	 * Useful to share and ship a full stream whilst hiding the staging actions in the middle
	 *
	 * @param reuse Reset to READY state the upstream chain while searching for the most ancient Action
	 * @param <E>
	 * @return new Action
	 */
	@SuppressWarnings("unchecked")
	public <E> CombineAction<E, O> combine(boolean reuse) {
		final Action<E, ?> subscriber = (Action<E, ?>) findOldestUpstream(Action.class, reuse);
		subscriber.subscription = null;
		return new CombineAction<E, O>(subscriber, this);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Action<I, O> capacity(long elements) {
		return (Action<I, O>) super.capacity(elements);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Action<I, O> env(Environment environment) {
		return (Action<I, O>) super.env(environment);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Action<I, O> keepAlive(boolean keepAlive) {
		return (Action<I, O>) super.keepAlive(keepAlive);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Action<I, O> ignoreErrors(boolean ignore) {
		return (Action<I, O>) super.ignoreErrors(ignore);
	}

	@Override
	public TimeoutAction<O> timeout(long timeout, Timer timer) {
		return connect(new TimeoutAction<O>(
				dispatcher,
				this,
				timer,
				timeout
		));
	}


	protected void requestUpstream(AtomicLong capacity, boolean terminated, long elements) {
		if (subscription != null && !terminated) {
			long currentCapacity = capacity.get();
			currentCapacity = currentCapacity == -1 ? elements : currentCapacity;
			if (!pause && (currentCapacity > 0)) {
				final long remaining = Math.min(currentCapacity, elements);
				onRequest(remaining);
			}
		}
	}

	@Override
	protected StreamSubscription<O> createSubscription(final Subscriber<? super O> subscriber, boolean reactivePull) {
		if (reactivePull) {
			return new StreamSubscription<O>(this, subscriber) {
				@Override
				public void request(long elements) {
					super.request(elements);
					requestUpstream(capacity, buffer.isComplete(), elements);
				}
			};
		} else {
			return new StreamSubscription.Firehose<O>(this, subscriber) {
				@Override
				public void request(long elements) {
					requestUpstream(capacity, isComplete(), elements);
				}
			};
		}
	}

	public Subscription getSubscription() {
		return subscription;
	}

	/**
	 * Utility to find the most ancient subscribed Action with an option to reset its state (e.g. in a case of retry()).
	 * Also used by debug() operation to render the complete flow from upstream.
	 *
	 * @param resetState
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <P extends Publisher<?>> P findOldestUpstream(Class<P> clazz, boolean resetState) {
		Action<?, ?> that = this;

		if (resetState) {
			resetState(that);
		}

		while (inspectPublisher(that, Action.class)) {

			that = (Action<?, ?>) ((StreamSubscription<?>) that.subscription).getPublisher();

			if (that != null) {
				if (resetState) {
					resetState(that);
				}

				if (FanInAction.class.isAssignableFrom(that.getClass())) {
					that = ((FanInAction<?, ?, ?>) that).dynamicMergeAction != null ? ((FanInAction<?, ?,
							?>) that).dynamicMergeAction : that;
				}
			}
		}

		if(inspectPublisher(that, clazz)) {
			return (P)((StreamSubscription<?>) that.subscription).getPublisher();
		}else {
			return (P)that;
		}
	}

	private boolean inspectPublisher(Action<?, ?> that, Class<?> actionClass){
		return that.subscription != null
				&& StreamSubscription.class.isAssignableFrom(that.subscription.getClass())
				&& ((StreamSubscription<?>) that.subscription).getPublisher() != null
				&& actionClass.isAssignableFrom(((StreamSubscription<?>) that.subscription).getPublisher().getClass());
	}

	@Override
	protected void removeSubscription(StreamSubscription<O> sub) {
		super.removeSubscription(sub);
		if (subscription != null) {
			subscription.cancel();
		}
	}

	protected void doSubscribe(Subscription subscription) {
		doPendingRequest();
	}

	protected long generateDemandFromPendingRequests() {
		return Math.min(pendingNextSignals, capacity);
	}

	protected void doComplete() {
		broadcastComplete();
	}

	abstract protected void doNext(I ev);

	protected void doError(Throwable ev) {
		if (!ignoreErrors) {
			broadcastError(ev);
		}
	}

	protected void doPendingRequest() {
		long toRequest = generateDemandFromPendingRequests();
		currentNextSignals = 0;

		if (toRequest > 0) {
			pendingNextSignals -= toRequest;
			subscription.request(toRequest);
		}
	}

	protected <E> void trySyncDispatch(E data, Consumer<E> action) {
		if (firehose &&
				(downstreamSubscription() != null && dispatcher.inContext())) {
			action.accept(data);
		} else {
			dispatch(data, action);
		}
	}

	protected void onRequest(final long n) {
		checkRequest(n);
		trySyncDispatch(n, requestConsumer);
	}

	protected void resetState(Action<?, ?> action) {
		action.state = State.READY;
		action.error = null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public String toString() {
		return "{" +
				"dispatcher=" + dispatcher.getClass().getSimpleName().replaceAll("Dispatcher", "") +
				((!SynchronousDispatcher.class.isAssignableFrom(dispatcher.getClass()) ? (":" + dispatcher.remainingSlots()) :
						"")) +
				", state=" + getState() +
				", max-capacity=" + getCapacity() +
				(subscription != null &&
						StreamSubscription.class.isAssignableFrom(subscription.getClass()) ?
						", subscription=" + subscription +
								", pending=" + pendingNextSignals +
								", currentNextSignals=" + currentNextSignals
						: (subscription != null ? ", subscription=" + subscription : "")
				) + '}';
	}

}
