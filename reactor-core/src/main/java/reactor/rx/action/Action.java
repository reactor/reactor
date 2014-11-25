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
package reactor.rx.action;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.alloc.Recyclable;
import reactor.core.Environment;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.routing.ArgumentConvertingConsumerInvoker;
import reactor.event.routing.ConsumerFilteringRouter;
import reactor.event.routing.Router;
import reactor.filter.PassThroughFilter;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Supplier;
import reactor.queue.CompletableLinkedQueue;
import reactor.queue.CompletableQueue;
import reactor.rx.Controls;
import reactor.rx.Stream;
import reactor.rx.StreamUtils;
import reactor.rx.action.support.NonBlocking;
import reactor.rx.action.support.SpecificationExceptions;
import reactor.rx.stream.HotStream;
import reactor.rx.subscription.DropSubscription;
import reactor.rx.subscription.FanOutSubscription;
import reactor.rx.subscription.PushSubscription;
import reactor.rx.subscription.ReactiveSubscription;
import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;

import javax.annotation.Nonnull;
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
		implements Processor<I, O>, Consumer<I>, Recyclable, Controls {

	//protected static final Logger log = LoggerFactory.getLogger(Action.class);

	public static final Router ROUTER = new ConsumerFilteringRouter(
			new PassThroughFilter(), new ArgumentConvertingConsumerInvoker(null)
	);


	/**
	 * onComplete, onError, request, onSubscribe are dispatched events, therefore up to capacity + 4 events can be
	 * in-flight
	 * stacking into a Dispatcher.
	 */
	public static final int RESERVED_SLOTS = 4;

	/**
	 * The upstream request tracker to avoid dispatcher overrun, based on the current {@link this#capacity}
	 */
	protected PushSubscription<I> upstreamSubscription;
	protected PushSubscription<O> downstreamSubscription;

	protected       Environment environment;
	protected final Dispatcher  dispatcher;

	protected long capacity;

	public Action() {
		this(Long.MAX_VALUE);
	}

	public Action(long capacity) {
		this(SynchronousDispatcher.INSTANCE, capacity);
	}

	public Action(Dispatcher dispatcher) {
		this(dispatcher, dispatcher != SynchronousDispatcher.INSTANCE ?
						dispatcher.backlogSize() - Action.RESERVED_SLOTS :
						Long.MAX_VALUE
		);
	}

	public Action(@Nonnull Dispatcher dispatcher, long batchSize) {
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
		return passthrough(dispatcher, dispatcher == SynchronousDispatcher.INSTANCE ? Long.MAX_VALUE : dispatcher
				.backlogSize());
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
	public void subscribe(final Subscriber<? super O> subscriber) {
		try {
			final NonBlocking asyncSubscriber = NonBlocking.class.isAssignableFrom(subscriber.getClass()) ?
					(NonBlocking) subscriber :
					null;

			final PushSubscription<O> subscription = createSubscription(subscriber,
					(null == asyncSubscriber || asyncSubscriber.getDispatcher() != dispatcher || asyncSubscriber.getCapacity() <
							capacity)
			);

			if (subscription == null)
				return;

			if (null != asyncSubscriber) {
				subscription.maxCapacity(asyncSubscriber.getCapacity());
			}

			subscribeWithSubscription(subscriber, subscription, asyncSubscriber == null);

		} catch (Exception e) {
			subscriber.onError(e);
		}
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		final boolean hasRequestTracker = upstreamSubscription != null;

		//if request tracker was connected to another subscription
		if (hasRequestTracker) {
			subscription.cancel();
			return;
		}

		upstreamSubscription = createTrackingSubscription(subscription);
		upstreamSubscription.maxCapacity(getCapacity());

		try {
			doSubscribe(subscription);
		} catch (Throwable t) {
			doError(t);
		}
	}

	@Override
	public void onNext(I ev) {
		trySyncDispatch(ev, this);
	}

	@Override
	public final void accept(I i) {
		try {
			if (upstreamSubscription != null) {
				upstreamSubscription.incrementCurrentNextSignals();
				doNext(i);
				if (upstreamSubscription != null
						&& (upstreamSubscription.shouldRequestPendingSignals())) {

					long left;
					if ((left = upstreamSubscription.clearPendingRequest()) > 0) {
						dispatch(left, upstreamSubscription);
					}
				}

			} else {
				doNext(i);
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
				} catch (Throwable t) {
					doError(t);
				}
			}
		});
	}

	@Override
	public void onError(Throwable cause) {
		trySyncDispatch(cause, new Consumer<Throwable>() {
			@Override
			public void accept(Throwable throwable) {
				if (upstreamSubscription != null) upstreamSubscription.clearPendingRequest();
				doError(throwable);
			}
		});
	}

	/**
	 * --------------------------------------------------------------------------------------------------------
	 * ACTION MODIFIERS
	 * --------------------------------------------------------------------------------------------------------
	 */

	@Override
	public Action<I, O> capacity(long elements) {
		if (dispatcher != SynchronousDispatcher.INSTANCE) {
			long dispatcherCapacity = dispatcher.backlogSize() - RESERVED_SLOTS;
			capacity = elements > dispatcherCapacity ? dispatcherCapacity : elements;
		} else {
			capacity = elements;
		}

		if (upstreamSubscription != null) {
			upstreamSubscription.maxCapacity(capacity);
		}
		return this;
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
	 * Send an element of parameterized type {link O} to all the attached {@link Subscriber}.
	 * A Stream must be in READY state to dispatch signals and will fail fast otherwise (IllegalStateException).
	 *
	 * @param ev the data to forward
	 * @since 2.0
	 */
	public void broadcastNext(final O ev) {
		//log.debug("event [" + ev + "] by: " + getClass().getSimpleName());
		PushSubscription<O> downstreamSubscription = this.downstreamSubscription;
		if (downstreamSubscription == null) {
			/*if (log.isTraceEnabled()) {
				log.trace("event [" + ev + "] dropped by: " + getClass().getSimpleName() + ":" + this);
			}*/
			return;
		}

		try {
			downstreamSubscription.onNext(ev);
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

		if (downstreamSubscription == null) {
			if (Environment.alive()) {
				Environment.get().routeError(throwable);
			}
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
		if (downstreamSubscription == null) {
			return;
		}

		try {
			downstreamSubscription.onComplete();
		} catch (Throwable throwable) {
			callError(downstreamSubscription, throwable);
		}
	}


	public void cancel() {
		if (upstreamSubscription != null) {
			upstreamSubscription.cancel();
			upstreamSubscription = null;
		}
	}

	@Override
	public void start() {
		requestMore(Long.MAX_VALUE);
	}

	/**
	 * Print a debugged form of the root action relative to this one. The output will be an acyclic directed graph of
	 * composed actions.
	 *
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public StreamUtils.StreamVisitor debug() {
		return StreamUtils.browse(findOldestUpstream(Action.class));
	}

	/**
	 * --------------------------------------------------------------------------------------------------------
	 * STREAM ACTION-SPECIFIC EXTENSIONS
	 * --------------------------------------------------------------------------------------------------------
	 */

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
	public final <E> Action<I, O> control(Stream<E> controlStream, final Consumer<Tuple2<Action<I, O>,
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
	public final Stream<O> onOverflowBuffer(final Supplier<? extends CompletableQueue<O>> queueSupplier) {
		return lift(new Function<Dispatcher, Action<? super O, ? extends O>>() {
			@Override
			public Action<? super O, ? extends O> apply(Dispatcher dispatcher) {
				HotStream<O> newStream = new HotStream<>(dispatcher, capacity);
				if (queueSupplier == null) {
					subscribeWithSubscription(newStream, new DropSubscription<O>(Action.this, newStream) {
						@Override
						public void request(long elements) {
							super.request(elements);
							requestUpstream(capacity, terminated, elements);
						}
					}, false);
				} else {
					subscribeWithSubscription(newStream,
							createSubscription(newStream, queueSupplier.get()), false);
				}
				return newStream;
			}
		});
	}

	@SuppressWarnings("unchecked")
	@Override
	public final <E> CombineAction<E, O> combine() {
		final Action<E, ?> subscriber = (Action<E, ?>) findOldestUpstream(Action.class);
		subscriber.upstreamSubscription = null;
		return new CombineAction<E, O>(subscriber, this);
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
	 * Utility to find the most ancient subscribed Action.
	 * Also used by debug() operation to render the complete flow from upstream.
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <P extends Publisher<?>> P findOldestUpstream(Class<P> clazz) {
		Action<?, ?> that = this;

		while (inspectPublisher(that, Action.class)) {

			that = (Action<?, ?>) that.upstreamSubscription.getPublisher();

			if (that != null) {

				if (FanInAction.class.isAssignableFrom(that.getClass())) {
					that = ((FanInAction) that).dynamicMergeAction != null ? ((FanInAction) that).dynamicMergeAction : that;
				}
			}
		}

		if (inspectPublisher(that, clazz)) {
			return (P) ((PushSubscription<?>) that.upstreamSubscription).getPublisher();
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
	public PushSubscription<I> getSubscription() {
		return upstreamSubscription;
	}


	/**
	 * Get the current action child subscription
	 *
	 * @return current child {@link reactor.rx.subscription.PushSubscription}
	 */
	public final PushSubscription<O> downstreamSubscription() {
		return downstreamSubscription;
	}

	public void replayChildRequests(long request) {
		if (request > 0) {
			downstreamSubscription.request(request);
		}
	}

	public long resetChildRequests() {
		return downstreamSubscription != null ? downstreamSubscription.clearPendingRequest() : 0l;
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
			onShutdown();
			return true;
		} else {
			PushSubscription<O> dsub = this.downstreamSubscription;
			if (FanOutSubscription.class.isAssignableFrom(dsub.getClass())) {
				FanOutSubscription<O> fsub =
						((FanOutSubscription<O>) this.downstreamSubscription);

				if (fsub.remove(subscription) && fsub.isEmpty()) {
					onShutdown();
					return true;
				}
			}
			return false;
		}
	}

	protected PushSubscription<O> createSubscription(final Subscriber<? super O> subscriber, boolean reactivePull) {
		return createSubscription(subscriber, reactivePull ? new CompletableLinkedQueue<O>() : null);
	}

	protected PushSubscription<O> createSubscription(final Subscriber<? super O> subscriber, CompletableQueue<O> queue) {
		if (queue != null) {
			return new ReactiveSubscription<O>(this, subscriber, queue) {

				@Override
				protected void onRequest(long elements) {
					if (upstreamSubscription == null) {
						updatePendingRequests(elements);
					} else {
						super.onRequest(elements);
						requestUpstream(capacity, buffer.isComplete(), elements);
					}
				}
			};
		} else {
			return new PushSubscription<O>(this, subscriber) {
				@Override
				protected void onRequest(long elements) {
					if (upstreamSubscription == null) {
						updatePendingRequests(elements);
					} else {
						requestUpstream(null, terminated, elements);
					}
				}
			};
		}
	}

	protected void requestUpstream(AtomicLong capacity, boolean terminated, long elements) {
		if (upstreamSubscription != null && !terminated) {
			if (capacity == null) {
				requestMore(elements);
				return;
			}
			long currentCapacity = capacity.get();
			currentCapacity = Long.MAX_VALUE == elements ? elements : currentCapacity;
			if (currentCapacity > 0) {
				final long remaining = Math.min(currentCapacity, elements);
				requestMore(remaining);
			}
		} else {
			if (downstreamSubscription != null) {
				downstreamSubscription.updatePendingRequests(elements);
			}
		}
	}

	@SuppressWarnings("unchecked")
	protected PushSubscription<I> createTrackingSubscription(Subscription subscription) {
		//If not a reactor push subscription, wrap within one
		if (!PushSubscription.class.isAssignableFrom(subscription.getClass())) {
			return PushSubscription.wrap(subscription, this);
		} else {
			return ((PushSubscription<I>) subscription);
		}
	}

	protected void doSubscribe(Subscription subscription) {
	}

	protected void doComplete() {
		if (downstreamSubscription == null) {
			cancel();
		}
		broadcastComplete();
	}

	abstract protected void doNext(I ev);

	protected void doError(Throwable ev) {
		if (downstreamSubscription == null) {
			cancel();
		}
		broadcastError(ev);
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
		if (downstreamSubscription == null ^ dispatcher.inContext()) {
			action.accept(data);
		} else {
			dispatch(data, action);
		}
	}

	@Override
	public void requestMore(final long n) {
		checkRequest(n);
		if (upstreamSubscription != null) {
			dispatch(n, upstreamSubscription);
		}
	}


	/**
	 * Subscribe a given subscriber and pairs it with a given subscription instead of letting the Stream pick it
	 * automatically.
	 * <p>
	 * This is mainly useful for libraries implementors, usually {@link this#lift(reactor.function.Function)} and
	 * {@link this#subscribe(org.reactivestreams.Subscriber)} are just fine.
	 *
	 * @param subscriber
	 * @param subscription
	 */
	protected final void subscribeWithSubscription(final Subscriber<? super O> subscriber, final PushSubscription<O>
			subscription, boolean dispatched) {
		try {
			if (addSubscription(subscription)) {
				if (!dispatched) {
					subscriber.onSubscribe(subscription);
				} else {
					dispatch(new Consumer<Void>() {
						@Override
						public void accept(Void aVoid) {
							try {
								subscriber.onSubscribe(subscription);
							} catch (Exception e) {
								subscriber.onError(e);
							}
						}
					});
				}
			} else {
				subscriber.onError(new IllegalStateException("The subscription cannot be linked to this Stream"));
			}
		} catch (Exception e) {
			subscriber.onError(e);
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

	private boolean inspectPublisher(Action<?, ?> that, Class<?> actionClass) {
		return that.upstreamSubscription != null
				&& ((PushSubscription<?>) that.upstreamSubscription).getPublisher() != null
				&& actionClass.isAssignableFrom(((PushSubscription<?>) that.upstreamSubscription).getPublisher().getClass());
	}

	private void callError(PushSubscription<O> subscription, Throwable cause) {
		subscription.onError(cause);
	}


	@Override
	public void recycle() {
		downstreamSubscription = null;
		environment = null;
		capacity = Long.MAX_VALUE;
		upstreamSubscription = null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public String toString() {
		return "{" +
				"dispatcher=" + dispatcher.getClass().getSimpleName().replaceAll("Dispatcher", "") +
				((!SynchronousDispatcher.class.isAssignableFrom(dispatcher.getClass()) ? (":" + dispatcher.remainingSlots()) :
						"")) +
				", max-capacity=" + (capacity == Long.MAX_VALUE ? "infinite" : capacity) +
				(upstreamSubscription != null ? ", " + upstreamSubscription : "") + '}';
	}

}
