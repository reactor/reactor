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
import reactor.alloc.Recyclable;
import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.lifecycle.Pausable;
import reactor.event.routing.ArgumentConvertingConsumerInvoker;
import reactor.event.routing.ConsumerFilteringRouter;
import reactor.event.routing.Router;
import reactor.filter.PassThroughFilter;
import reactor.function.Consumer;
import reactor.queue.CompletableQueue;
import reactor.rx.Stream;
import reactor.rx.StreamUtils;
import reactor.rx.action.support.NonBlocking;
import reactor.rx.action.support.SpecificationExceptions;
import reactor.rx.stream.HotStream;
import reactor.rx.subscription.FanOutSubscription;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.ReactiveSubscription;
import reactor.timer.Timer;
import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
public abstract class Action<I, O> extends Stream<O>
		implements Processor<I, O>, Consumer<I>, NonBlocking, Pausable, Recyclable {

	//protected static final Logger log = LoggerFactory.getLogger(Action.class);

	public static final Router ROUTER = new ConsumerFilteringRouter(
			new PassThroughFilter(), new ArgumentConvertingConsumerInvoker(null)
	);

	public static enum FinalState {
		ERROR,
		COMPLETE,
		SHUTDOWN
	}

	/**
	 * onComplete, onError, request, onSubscribe are dispatched events, therefore up to capacity + 4 events can be
	 * in-flight
	 * stacking into a Dispatcher.
	 */
	public static final int RESERVED_SLOTS = 4;


	protected long    pendingNextSignals = 0;
	protected long    currentNextSignals = 0;
	protected boolean firehose           = false;
	protected boolean pause = false;
	protected Throwable           error;

	protected PushSubscription<O> downstreamSubscription;

	protected FinalState finalState;

	protected Subscription subscription;
	protected Dispatcher   dispatcher;

	protected long capacity;
	protected boolean keepAlive    = false;
	protected boolean ignoreErrors = false;
	protected Environment environment;

	/**
	 * The upstream request tracker to avoid dispatcher overrun, based on the current {@link this#capacity}
	 */

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

	public Action() {
		this(Long.MAX_VALUE);
	}

	public Action(long capacity) {
		this(SynchronousDispatcher.INSTANCE, capacity);
	}

	public Action(Dispatcher dispatcher) {
		this(dispatcher, dispatcher.backlogSize() - Action.RESERVED_SLOTS);
	}

	public Action(@Nonnull Dispatcher dispatcher, long batchSize) {
		this.keepAlive = false;
		this.capacity = batchSize;
		this.dispatcher = dispatcher;
	}

	public static void checkRequest(long n) {
		if (n <= 0l) {
			throw SpecificationExceptions.spec_3_09_exception(n);
		}
	}

	/**
	 * A simple Action that can be used to broadcast Stream signals such as dispatcher or capacity.
	 *
	 * @param dispatcher the dispatcher to run on
	 * @param <O>        the streamed data type
	 * @return a new Action subscribed to this one
	 */
	public static <O> HotStream<O> passthrough(Dispatcher dispatcher) {
		return passthrough(dispatcher, dispatcher.backlogSize());
	}

	/**
	 * A simple Action that can be used to broadcast Stream signals such as dispatcher or capacity.
	 *
	 * @param dispatcher the dispatcher to run on
	 * @param capacity   the capacity to assign {@link this#capacity(long)}
	 * @param <O>        the streamed data type
	 * @return a new Action subscribed to this one
	 */
	public static <O> HotStream<O> passthrough(Dispatcher dispatcher, long capacity) {
		return new HotStream<>(dispatcher, capacity);
	}

	/**
	 * --------------------------------------------------------------------------------------------------------
	 * ACTION SIGNAL HANDLING
	 * --------------------------------------------------------------------------------------------------------
	 */

	@Override
	public void subscribe(Subscriber<? super O> subscriber) {
		subscribeWithSubscription(subscriber, createSubscription(subscriber,
						subscription == null || !NonBlocking.class.isAssignableFrom(subscriber.getClass()))
		);
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		if (this.subscription != null) {
			subscription.cancel();
			return;
		}

		this.subscription = subscription;
		this.finalState = null;
		this.firehose = !ReactiveSubscription.class.isAssignableFrom(subscription.getClass());


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
	public void onNext(I ev) {
		trySyncDispatch(ev, this);
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
	public void onComplete() {
		trySyncDispatch(null, new Consumer<Void>() {
			@Override
			public void accept(Void any) {
				try {
					doComplete();
					if (!keepAlive) {
						cancel();
					}
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

	/**
	 * --------------------------------------------------------------------------------------------------------
	 * ACTION MODIFIERS
	 * --------------------------------------------------------------------------------------------------------
	 */

	/**
	 * Bind the stream to a given {@param elements} volume of in-flight data:
	 * - An {@link Action} will request up to the defined volume upstream.
	 * - An {@link Action} will track the pending requests and fire up to {@param elements} when the previous volume has
	 * been processed.
	 * - A {@link BatchAction} and any other size-bound action will be limited to the defined volume.
	 * <p>
	 * <p>
	 * A stream capacity can't be superior to the underlying dispatcher capacity: if the {@param elements} overflow the
	 * dispatcher backlog size, the capacity will be aligned automatically to fit it. A warning message should signal
	 * such behavior.
	 * RingBufferDispatcher will for instance limit to a power of 2 size up to {@literal Integer.MAX_VALUE},
	 * where a Stream can be sized up to {@literal Long.MAX_VALUE} in flight data.
	 * <p>
	 * <p>
	 * When the stream receives more elements than requested, incoming data is eventually staged in the eventual {@link
	 * org.reactivestreams.Subscription}.
	 * The subscription can react differently according to the implementation in-use,
	 * the default strategy is as following:
	 * - The first-level of pair compositions Stream->Action will overflow data in a {@link reactor.queue
	 * .CompletableQueue},
	 * ready to be polled when the action fire the pending requests.
	 * - The following pairs of Action->Action will synchronously pass data
	 * - Any pair of Stream->Subscriber or Action->Subscriber will behave as with the root Stream->Action pair rule.
	 * - {@link this#onOverflowBuffer()} force this staging behavior, with a possibilty to pass a {@link reactor.queue
	 * .PersistentQueue}
	 *
	 * @param elements
	 * @return {@literal this}
	 */
	public Action<I, O> capacity(long elements) {
		this.capacity = elements > (dispatcher.backlogSize() - Action.RESERVED_SLOTS) ?
				dispatcher.backlogSize() - Action.RESERVED_SLOTS : elements;
		/*if (log.isTraceEnabled() && capacity != elements) {
			log.trace(" The assigned capacity is now {}. The Stream altered the requested maximum capacity {} to not " +
							"overrun" +
							" " +
							"its Dispatcher which supports " +
							"up to {} slots for next signals, minus {} slots error|" +
							"complete|subscribe|request.",
					capacity, elements, dispatcher.backlogSize(), Action.RESERVED_SLOTS);
		}*/
		return this;
	}

	@Override
	public final Action<I, O> dispatchOn(@Nonnull final Environment environment) {
		return dispatchOn(environment, environment.getDefaultDispatcher());
	}

	@Override
	public final Action<I, O> dispatchOn(@Nonnull final Dispatcher dispatcher) {
		return dispatchOn(null, dispatcher);
	}

	/**
	 * Re-assign the current dispatcher to run on and evaluate the capacity against its backlog through {@link
	 * this#capacity(long)}
	 *
	 * @param dispatcher the new dispatcher
	 * @return this
	 */
	@Override
	public Action<I, O> dispatchOn(@Nullable Environment environment, @Nonnull final Dispatcher dispatcher) {
		Assert.state(dispatcher.supportsOrdering(), "Dispatcher provided doesn't support event ordering. " +
				" Refer to #parallel() method. ");
		this.dispatcher = dispatcher;
		this.environment = environment != null ? environment : this.environment;

		//refresh capacity vs dispatcher backlog
		capacity(capacity);
		return this;
	}


	/**
	 * Instruct the action to request upstream subscription if any for {@link this#capacity} elements. If the dispatcher
	 * is asynchronous (RingBufferDispatcher for instance), it will proceed the request asynchronously as well.
	 */
	public void drain() {
		drain(capacity);
	}

	/**
	 * Instruct the action to request upstream subscription if any for N elements.
	 *
	 * @param n the number of elements to request
	 */
	public void drain(long n) {
		if (subscription != null && !pause) {
			dispatch(n, requestConsumer);
		}
	}

	/**
	 * Update the environment used by this {@link Stream}
	 *
	 * @param environment the new environment to use
	 * @return {@literal this}
	 */
	public Action<I, O> env(Environment environment) {
		this.environment = environment;
		return this;
	}

	/**
	 * Update the keep-alive property used by this {@link Stream}. When kept-alive, the stream will not shutdown
	 * automatically on complete. Shutdown state is observed when the last subscriber is cancelled.
	 *
	 * @param keepAlive to either automatically shutdown and {@link this#cancel()} any upstream subscription when no
	 *                  downstream is consuming anymore.
	 * @return {@literal this}
	 */
	public Action<I, O> keepAlive(boolean keepAlive) {
		this.keepAlive = keepAlive;
		return this;
	}

	/**
	 * Update the ignore-errors property used by this {@link Stream}. When ignoring, the stream will not terminate on
	 * error
	 *
	 * @param ignore to either ignore errors and keep accepting signal or terminate with the error.
	 * @return {@literal this}
	 */
	public Action<I, O> ignoreErrors(boolean ignore) {
		this.ignoreErrors = ignore;
		return this;
	}


	/**
	 * Send an element of parameterized type {link O} to all the attached {@link Subscriber}.
	 * A Stream must be in READY state to dispatch signals and will fail fast otherwise (IllegalStateException).
	 *
	 * @param ev the data to forward
	 * @since 2.0
	 */
	public void broadcastNext(final O ev) {
		//log.debug("event [" + ev + "] by: " + getClass().getSimpleName());
		if (!isRunning() || downstreamSubscription == null) {
			/*if (log.isTraceEnabled()) {
				log.trace("event [" + ev + "] dropped by: " + getClass().getSimpleName() + ":" + this);
			}*/
			return;
		}

		try {
			downstreamSubscription.onNext(ev);

			if (finalState == FinalState.COMPLETE) {
				downstreamSubscription.onComplete();
			}
		} catch (Throwable throwable) {
			callError(downstreamSubscription, throwable);
		}
	}

	/**
	 * Send an error to all the attached {@link Subscriber}.
	 * A Stream must be in READY state to dispatch signals and will fail fast otherwise (IllegalStateException).
	 *
	 * @param throwable the error to forward
	 * @since 2.0
	 */
	public void broadcastError(final Throwable throwable) {
		//log.debug("event [" + throwable + "] by: " + getClass().getSimpleName());
		/*if (!isRunning()) {
			if (log.isTraceEnabled()) {
				log.trace("error dropped by: " + getClass().getSimpleName() + ":" + this, throwable);
			}
		}*/

		if (!ignoreErrors) {
			finalState = FinalState.ERROR;
			error = throwable;
		}

		if (downstreamSubscription == null) {
			/*log.error(this.getClass().getSimpleName() + " > broadcastError:" + this, new Exception(debug().toString(),
					throwable)); */
			return;
		}

		downstreamSubscription.onError(throwable);
	}

	/**
	 * Send a complete event to all the attached {@link Subscriber} ONLY IF the underlying state is READY.
	 * Unlike {@link #broadcastNext(Object)} and {@link #broadcastError(Throwable)} it will simply ignore the signal.
	 *
	 * @since 2.0
	 */
	public void broadcastComplete() {
		//log.debug("event [complete] by: " + getClass().getSimpleName());
		if (finalState != null) {
			/*if (log.isTraceEnabled()) {
				log.trace("Complete signal dropped by: " + getClass().getSimpleName() + ":" + this);
			}*/
			return;
		}

		if (downstreamSubscription == null) {
			finalState = FinalState.COMPLETE;
			return;
		}

		try {
			downstreamSubscription.onComplete();
		} catch (Throwable throwable) {
			callError(downstreamSubscription, throwable);
		}

		finalState = FinalState.COMPLETE;
	}


	@Override
	public Action<I, O> cancel() {
		if (subscription != null) {
			subscription.cancel();
			subscription = null;
		}

		//Force state shutdown
		this.finalState = FinalState.SHUTDOWN;
		return this;
	}

	@Override
	public Action<I, O> pause() {
		pause = true;
		return this;
	}

	@Override
	public Action<I, O> resume() {
		pause = false;
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public StreamUtils.StreamVisitor debug() {
		return StreamUtils.browse(findOldestUpstream(Action.class, false));
	}

	/**
	 * --------------------------------------------------------------------------------------------------------
	 * STREAM ACTION-SPECIFIC EXTENSIONS
	 * --------------------------------------------------------------------------------------------------------
	 */

	/**
	 * Subscribe an {@link Action} to the actual pipeline.
	 * <p>
	 * Additionally to producing events (error,complete,next and eventually flush), it will take care of setting the
	 * environment if available.
	 * It will also give an initial capacity size used for {@link org.reactivestreams.Subscription#request(long)}
	 * ONLY
	 * IF the passed action capacity is not the default Long.MAX_VALUE ({@see this#getCapacity()(elements)}.
	 * Current KeepAlive value is also assigned
	 * <p>
	 * Reactive Extensions patterns also dubs this operation "lift".
	 * The operation is returned for functional-style chaining.
	 * <p>
	 * If the new action dispatcher is different, the new action will take
	 * care of buffering incoming data into a StreamSubscription. Otherwise default behavior is picked:
	 * Push synchronous subscription is the parent stream != null.
	 *
	 * @param action the processor to subscribe.
	 * @param <E>    the {@link Action} type
	 * @param <A>    the {@link Action} output type
	 * @return the passed action
	 * @see {@link org.reactivestreams.Publisher#subscribe(org.reactivestreams.Subscriber)}
	 * @since 2.0
	 */
	@Override
	public final <A, E extends Action<? super O, ? extends A>> E connect(@Nonnull final E action) {
		if ((action.capacity == Long.MAX_VALUE && getCapacity() != Long.MAX_VALUE) ||
				action.capacity == action.dispatcher.backlogSize() - Action.RESERVED_SLOTS) {
			action.capacity(capacity);
		}

		if (action.environment == null) {
			action.env(environment);
		}

		if (!action.keepAlive) {
			action.keepAlive(keepAlive);
		}

		if (action.dispatcher != this.dispatcher) {
			this.subscribeWithSubscription(action, createSubscription(action, true));
		} else {
			this.subscribe(action);
		}
		return action;
	}

	/**
	 * Consume a Stream to allow for dynamic {@link Action} update. Everytime
	 * the {@param controlStream} receives a next signal, the current Action and the input data will be published as a
	 * {@link reactor.tuple.Tuple2} to the attached {@param controller}.
	 * <p>
	 * This is particulary useful to dynamically adapt the {@link Stream} instance : capacity(), pause(), resume()...
	 *
	 * @param controlStream The consumed stream, each signal will trigger the passed controller
	 * @param controller    The consumer accepting a pair of Stream and user-provided signal type
	 * @return the current {@link Stream} instance
	 * @since 2.0
	 */
	public final  <E> Action<I, O> control(Stream<E> controlStream, final Consumer<Tuple2<Action<I, O>,
			? super E>> controller) {
		final Action<I, O> thiz = this;
		controlStream.consume(new Consumer<E>() {
			@Override
			public void accept(E e) {
				controller.accept(Tuple.of(thiz, e));
			}
		});
		return this;
	}

	@Override
	public final Action<O, O> onOverflowBuffer(CompletableQueue<O> queue) {
		FlowControlAction<O> stream = new FlowControlAction<O>(getDispatcher());
		if (queue != null) {
			stream.capacity(capacity).env(environment);
			stream.keepAlive(keepAlive);
			subscribeWithSubscription(stream, createSubscription(stream, true).toReactiveSubscription(queue));
		} else {
			connect(stream);
		}
		return stream;
	}

	@Override
	@SuppressWarnings("unchecked")
	public final  <T, V> Action<Publisher<? extends T>, V> fanIn(FanInAction<T, V, ? extends FanInAction.InnerSubscriber<T,
			V>> fanInAction) {
		Action<?, Publisher<T>> thiz = (Action<?, Publisher<T>>) this;

		Action<Publisher<? extends T>, V> innerMerge = new DynamicMergeAction<T, V>(getDispatcher(), fanInAction);
		innerMerge.capacity(capacity).env(environment).keepAlive(keepAlive);

		thiz.subscribeWithSubscription(innerMerge, thiz.createSubscription(innerMerge, true));
		return innerMerge;
	}

	/**
	 * Combine the most ancient upstream action to act as the {@link org.reactivestreams.Subscriber} input component and
	 * the current action to act as the {@link org.reactivestreams.Publisher}.
	 * <p>
	 * Useful to share and ship a full stream whilst hiding the staging actions in the middle
	 *
	 * @param <E> the type of the most ancien action input.
	 * @return new Action
	 */
	public final <E> CombineAction<E, O> combine() {
		return combine(false);
	}

	/**
	 * Combine the most ancient upstream action to act as the {@link org.reactivestreams.Subscriber} input component and
	 * the current action to act as the {@link org.reactivestreams.Publisher}.
	 * <p>
	 * Useful to share and ship a full stream whilst hiding the staging actions in the middle
	 *
	 * @param reuse Reset to READY state the upstream chain while searching for the most ancient Action
	 * @param <E> the type of the most ancien action input.
	 * @return new Action
	 */
	@SuppressWarnings("unchecked")
	public final  <E> CombineAction<E, O> combine(boolean reuse) {
		final Action<E, ?> subscriber = (Action<E, ?>) findOldestUpstream(Action.class, reuse);
		subscriber.subscription = null;
		return new CombineAction<E, O>(subscriber, this);
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

	/**
	 * Create a consumer that broadcast complete signal from any accepted value.
	 *
	 * @return a new {@link Consumer} ready to forward complete signal to this stream
	 * @since 2.0
	 */
	public final Consumer<?> toBroadcastCompleteConsumer() {
		return new Consumer<Object>() {
			@Override
			public void accept(Object o) {
				broadcastComplete();
			}
		};
	}


	/**
	 * Create a consumer that broadcast next signal from accepted values.
	 *
	 * @return a new {@link Consumer} ready to forward values to this stream
	 * @since 2.0
	 */
	public final Consumer<O> toBroadcastNextConsumer() {
		return new Consumer<O>() {
			@Override
			public void accept(O o) {
				broadcastNext(o);
			}
		};
	}

	/**
	 * Create a consumer that broadcast error signal from any accepted value.
	 *
	 * @return a new {@link Consumer} ready to forward error to this stream
	 * @since 2.0
	 */
	public final Consumer<Throwable> toBroadcastErrorConsumer() {
		return new Consumer<Throwable>() {
			@Override
			public void accept(Throwable o) {
				broadcastError(o);
			}
		};
	}

	/**
	 * Utility to find the most ancient subscribed Action with an option to reset its state (e.g. in a case of retry()).
	 * Also used by debug() operation to render the complete flow from upstream.
	 *
	 * @param resetState Will reset any action terminal state
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <P extends Publisher<?>> P findOldestUpstream(Class<P> clazz, boolean resetState) {
		Action<?, ?> that = this;

		if (resetState) {
			resetState(that);
		}

		while (inspectPublisher(that, Action.class)) {

			that = (Action<?, ?>) ((PushSubscription<?>) that.subscription).getPublisher();

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

		if (inspectPublisher(that, clazz)) {
			return (P) ((PushSubscription<?>) that.subscription).getPublisher();
		} else {
			return (P) that;
		}
	}

	/**
	 * --------------------------------------------------------------------------------------------------------
	 * ACTION STATE
	 * --------------------------------------------------------------------------------------------------------
	 */

	@Override
	public final Dispatcher getDispatcher() {
		return dispatcher;
	}

	@Override
	public final Environment getEnvironment() {
		return environment;
	}


	@Override
	public final long getCapacity() {
		return capacity;
	}

	/**
	 * Get the current upstream subscription if any
	 *
	 * @return current {@link org.reactivestreams.Subscription}
	 */
	public Subscription getSubscription() {
		return subscription;
	}

	/**
	 * Get the current action state.
	 *
	 * @return current {@link reactor.rx.action.Action.FinalState}
	 */
	public final FinalState getFinalState() {
		return finalState;
	}

	/**
	 * Get the current action error.
	 *
	 * @return current {@link java.lang.Throwable} error if any
	 */
	public final Throwable getError() {
		return error;
	}

	/**
	 * Get the current action pause status
	 *
	 * @return current {@link this#pause} value
	 */
	public final boolean isPaused() {
		return pause;
	}

	/**
	 * Get the current action child subscription
	 *
	 * @return current child {@link reactor.rx.subscription.PushSubscription}
	 */
	public final PushSubscription<O> downstreamSubscription() {
		return downstreamSubscription;
	}


	/**
	 * Is the current Action willing to accept new signals
	 *
	 * @return true if signals can be dispatched
	 */
	public final boolean isRunning() {
		return finalState == null;
	}


	/**
	 * --------------------------------------------------------------------------------------------------------
	 * INTERNALS
	 * --------------------------------------------------------------------------------------------------------
	 */

	@Override
	public boolean cleanSubscriptionReference(final PushSubscription<O> subscription) {
		if (this.downstreamSubscription == null) return false;

		if (subscription == this.downstreamSubscription) {
			this.downstreamSubscription = null;
			if (!keepAlive) {
				onShutdown();
			}
			return true;
		} else {
			PushSubscription<O> dsub = this.downstreamSubscription;
			if (FanOutSubscription.class.isAssignableFrom(dsub.getClass())) {
				FanOutSubscription<O> fsub =
						((FanOutSubscription<O>) this.downstreamSubscription);

				if (fsub.remove(subscription) && fsub.isEmpty() && !keepAlive) {
					onShutdown();
					return true;
				}
			}
			return false;
		}
	}

	@Override
	public void recycle() {
		downstreamSubscription = null;
		finalState = null;
		environment = null;
		firehose = false;
		dispatcher = SynchronousDispatcher.INSTANCE;
		capacity = Long.MAX_VALUE;
		keepAlive = false;
		error = null;
		pause = false;
		subscription = null;
		currentNextSignals = 0;
		pendingNextSignals = 0;
		ignoreErrors = false;
	}

	@Override
	@SuppressWarnings("unchecked")
	public String toString() {
		return "{" +
				"dispatcher=" + dispatcher.getClass().getSimpleName().replaceAll("Dispatcher", "") +
				((!SynchronousDispatcher.class.isAssignableFrom(dispatcher.getClass()) ? (":" + dispatcher.remainingSlots()) :
						"")) +
				(getFinalState() != null ? ", state=" + getFinalState() : "") +
				", ka=" + keepAlive +
				", max-capacity=" + getCapacity() +
				(subscription != null &&
						ReactiveSubscription.class.isAssignableFrom(subscription.getClass()) ?
						", subscription=" + subscription +
								", pending=" + pendingNextSignals +
								", currentNextSignals=" + currentNextSignals
						: (subscription != null ? ", subscription=" + subscription : "")
				) + '}';
	}

	protected PushSubscription<O> createSubscription(final Subscriber<? super O> subscriber, boolean reactivePull) {
		if (reactivePull) {
			return new ReactiveSubscription<O>(this, subscriber) {

				@Override
				public void request(long elements) {
					super.request(elements);
					requestUpstream(capacity, buffer.isComplete(), elements);
				}
			};
		} else {
			return new PushSubscription<O>(this, subscriber) {
				@Override
				public void request(long elements) {
					requestUpstream(null, false, elements);
				}
			};
		}
	}

	protected void requestUpstream(AtomicLong capacity, boolean terminated, long elements) {
		if (subscription != null && !terminated) {
			if (capacity == null) {
				onRequest(elements);
				return;
			}
			long currentCapacity = capacity.get();
			currentCapacity = currentCapacity == -1 ? elements : currentCapacity;
			if (!pause && (currentCapacity > 0)) {
				final long remaining = Math.min(currentCapacity, elements);
				onRequest(remaining);
			}
		}
	}

	protected void doSubscribe(Subscription subscription) {
		doPendingRequest();
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
		if (pendingNextSignals == 0) return;

		long toRequest = generateDemandFromPendingRequests();
		currentNextSignals = 0;

		if (toRequest > 0) {
			pendingNextSignals -= toRequest;
			subscription.request(toRequest);
		}
	}

	protected final long generateDemandFromPendingRequests() {
		return Math.min(pendingNextSignals, capacity);
	}

	protected final void dispatch(Consumer<Void> action) {
		dispatch(null, action);
	}

	protected final <E> void dispatch(E data, Consumer<E> action) {
		if (SynchronousDispatcher.INSTANCE == dispatcher) {
			action.accept(data);
		} else {
			dispatcher.dispatch(this, data, null, null, ROUTER, action);
		}
	}


	protected final <E> void trySyncDispatch(E data, Consumer<E> action) {
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

	/**
	 * Subscribe a given subscriber and pairs it with a given subscription instead of letting the Stream pick it
	 * automatically.
	 * <p>
	 * This is mainly useful for libraries implementors, usually {@link this#connect(reactor.rx.action.Action)} and
	 * {@link this#subscribe(org.reactivestreams.Subscriber)} are just fine.
	 *
	 * @param subscriber
	 * @param subscription
	 */
	protected void subscribeWithSubscription(final Subscriber<? super O> subscriber, final PushSubscription<O>
			subscription) {
		if (finalState == null && addSubscription(subscription)) {
			if (NonBlocking.class.isAssignableFrom(subscriber.getClass())) {
				subscriber.onSubscribe(subscription);
			} else {
				dispatch(new Consumer<Void>() {
					@Override
					public void accept(Void aVoid) {
						subscriber.onSubscribe(subscription);
					}
				});
			}
		} else if (finalState == FinalState.COMPLETE) {
			subscriber.onComplete();
		} else if (finalState == FinalState.SHUTDOWN) {
			subscriber.onError(new IllegalStateException("Publisher has shutdown"));
		} else if (finalState == FinalState.ERROR) {
			subscriber.onError(error);
		}
	}

	@SuppressWarnings("unchecked")
	protected boolean addSubscription(final PushSubscription<O> subscription) {
		PushSubscription<O> currentSubscription = this.downstreamSubscription;
		if (currentSubscription == null) {
			this.downstreamSubscription = subscription;
			return true;
		} else if (currentSubscription.equals(subscription)) {
			subscription.onError(SpecificationExceptions.spec_2_12_exception());
			return false;
		} else if (FanOutSubscription.class.isAssignableFrom(currentSubscription.getClass())) {
			if (((FanOutSubscription<O>) currentSubscription).contains(subscription)) {
				subscription.onError(SpecificationExceptions.spec_2_12_exception());
				return false;
			} else {
				return ((FanOutSubscription<O>) currentSubscription).add(subscription);
			}
		} else {
			this.downstreamSubscription = new FanOutSubscription<O>(this, currentSubscription, subscription);
			return true;
		}
	}

	protected void onShutdown() {
		cancel();
	}

	protected void resetState(Action<?, ?> action) {
		action.finalState = null;
		action.error = null;
	}

	private boolean inspectPublisher(Action<?, ?> that, Class<?> actionClass) {
		return that.subscription != null
				&& PushSubscription.class.isAssignableFrom(that.subscription.getClass())
				&& ((PushSubscription<?>) that.subscription).getPublisher() != null
				&& actionClass.isAssignableFrom(((PushSubscription<?>) that.subscription).getPublisher().getClass());
	}

	protected void setFinalState(FinalState finalState) {
		this.finalState = finalState;
	}

	private void callError(PushSubscription<O> subscription, Throwable cause) {
		subscription.onError(cause);
	}

}
