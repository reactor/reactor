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

package reactor.rx;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.dispatch.TailRecurseDispatcher;
import reactor.core.queue.CompletableBlockingQueue;
import reactor.core.queue.CompletableLinkedQueue;
import reactor.core.queue.CompletableQueue;
import reactor.core.support.Assert;
import reactor.core.support.Exceptions;
import reactor.core.support.NonBlocking;
import reactor.fn.*;
import reactor.fn.support.Tap;
import reactor.fn.timer.Timer;
import reactor.fn.tuple.Tuple2;
import reactor.fn.tuple.TupleN;
import reactor.rx.action.Action;
import reactor.rx.action.CompositeAction;
import reactor.rx.action.Control;
import reactor.rx.action.Signal;
import reactor.rx.action.aggregation.*;
import reactor.rx.action.combination.*;
import reactor.rx.action.conditional.ExistsAction;
import reactor.rx.action.control.*;
import reactor.rx.action.error.*;
import reactor.rx.action.filter.*;
import reactor.rx.action.metrics.CountAction;
import reactor.rx.action.metrics.ElapsedAction;
import reactor.rx.action.metrics.TimestampAction;
import reactor.rx.action.passive.CallbackAction;
import reactor.rx.action.passive.LoggerAction;
import reactor.rx.action.support.TapAndControls;
import reactor.rx.action.terminal.AdaptiveConsumerAction;
import reactor.rx.action.terminal.ConsumerAction;
import reactor.rx.action.transformation.*;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.stream.GroupedStream;
import reactor.rx.stream.LiftStream;
import reactor.rx.subscription.PushSubscription;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Base class for components designed to provide a succinct API for working with future values.
 * Provides base functionality and an internal contract for subclasses that make use of
 * the {@link #map(reactor.fn.Function)} and {@link #filter(reactor.fn.Predicate)} methods.
 * <p>
 * A Stream can be implemented to perform specific actions on callbacks (doNext,doComplete,doError,doOnSubscribe).
 * It is an asynchronous boundary and will run the callbacks using the input {@link Dispatcher}. Stream can
 * eventually produce a result {@code <O>} and will offer cascading over its own subscribers.
 * <p>
 * *
 * Typically, new {@code Stream} aren't created directly. To create a {@code Stream},
 * create a {@link Streams} and configure it with the appropriate {@link Environment},
 * {@link Dispatcher}, and other settings.
 *
 * @param <O> The type of the output values
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1, 2.0
 */
public abstract class Stream<O> implements Publisher<O>, NonBlocking {


	protected Stream() {
	}

	/**
	 * Cast the current Stream flowing data type into a target class type.
	 *
	 * @param <E> the {@link Action} output type
	 * @return the current {link Stream} instance casted
	 * @since 2.0
	 */
	@SuppressWarnings({"unchecked", "unused"})
	public final <E> Stream<E> cast(@Nonnull final Class<E> stream) {
		return (Stream<E>) this;
	}

	/**
	 * Defer the subscription of an {@link Action} to the actual pipeline.
	 * Terminal operations such as {@link this#consume(reactor.fn.Consumer)} will start the subscription chain.
	 * It will listen for current Stream signals and will be eventually producing signals as well (subscribe,error,
	 * complete,next).
	 * <p>
	 * The action is returned for functional-style chaining.
	 *
	 * @param <V>    the {@link reactor.rx.action.Action} output type
	 * @param action the function to map a provided dispatcher to a fresh Action to subscribe.
	 * @return the passed action
	 * @see {@link org.reactivestreams.Publisher#subscribe(org.reactivestreams.Subscriber)}
	 * @since 2.0
	 */
	public <V> Stream<V> lift(@Nonnull final Supplier<? extends Action<O, V>>
			                          action) {
		return new LiftStream<>(this, action);
	}

	/**
	 * Assign an error handler to exceptions of the given type. Will not stop error propagation, use when(class,
	 * publisher), retry, ignoreError or recover to actively deal with the exception
	 *
	 * @param exceptionType the type of exceptions to handle
	 * @param onError       the error handler for each exception
	 * @param <E>           type of the exception to handle
	 * @return {@literal new Stream}
	 */
	@SuppressWarnings("unchecked")
	public final <E extends Throwable> Stream<O> when(@Nonnull final Class<E> exceptionType,
	                                                  @Nonnull final Consumer<E> onError) {
		return lift(new Supplier<Action<O, O>>() {

			@Override
			public Action<O, O> get() {
				return new ErrorAction<O, E>(exceptionType, onError, null);
			}
		});
	}

	/**
	 * Assign an error handler that will pass eventual associated values and exceptions of the given type.
	 * Will not stop error propagation, use when(class,
	 * publisher), retry, ignoreError or recover to actively deal with the exception.
	 *
	 * @param exceptionType the type of exceptions to handle
	 * @param onError       the error handler for each exception
	 * @param <E>           type of the exception to handle
	 * @return {@literal new Stream}
	 */
	@SuppressWarnings("unchecked")
	public final <E extends Throwable> Stream<O> observeError(@Nonnull final Class<E> exceptionType,
	                                                          @Nonnull final BiConsumer<Object, ? super E> onError) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new ErrorWithValueAction<O, E>(exceptionType, onError, null);
			}
		});
	}

	/**
	 * Subscribe to a fallback publisher when any exception occurs.
	 *
	 * @param fallback the error handler for each exception
	 * @return {@literal new Stream}
	 */
	public final Stream<O> onErrorResumeNext(@Nonnull final Publisher<? extends O> fallback) {
		return onErrorResumeNext(Throwable.class, fallback);
	}

	/**
	 * Subscribe to a fallback publisher when exceptions of the given type occur, otherwise propagate the error.
	 *
	 * @param exceptionType the type of exceptions to handle
	 * @param fallback      the error handler for each exception
	 * @param <E>           type of the exception to handle
	 * @return {@literal new Stream}
	 */
	@SuppressWarnings("unchecked")
	public final <E extends Throwable> Stream<O> onErrorResumeNext(@Nonnull final Class<E> exceptionType,
	                                                               @Nonnull final Publisher<? extends O> fallback) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new ErrorAction<O, E>(exceptionType, null, fallback);
			}
		});
	}

	/**
	 * Produce a default value if any exception occurs.
	 *
	 * @param fallback the error handler for each exception
	 * @return {@literal new Stream}
	 */
	public final Stream<O> onErrorReturn(@Nonnull final Function<Throwable, ? extends O> fallback) {
		return onErrorReturn(Throwable.class, fallback);
	}

	/**
	 * Produce a default value when exceptions of the given type occur, otherwise propagate the error.
	 *
	 * @param exceptionType the type of exceptions to handle
	 * @param fallback      the error handler for each exception
	 * @param <E>           type of the exception to handle
	 * @return {@literal new Stream}
	 */
	@SuppressWarnings("unchecked")
	public final <E extends Throwable> Stream<O> onErrorReturn(@Nonnull final Class<E> exceptionType,
	                                                           @Nonnull final Function<E, ? extends O> fallback) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new ErrorReturnAction<O, E>(exceptionType, fallback);
			}
		});
	}


	/**
	 * Transform the incoming onSubscribe, onNext, onError and onComplete signals into {@link reactor.rx.action.Signal}.
	 * Since the error is materialized as a {@code Signal}, the propagation will be stopped.
	 * Complete signal will first emit a {@code Signal.complete()} and then effectively complete the stream.
	 *
	 * @return {@literal new Stream}
	 */
	public final Stream<Signal<O>> materialize() {
		return lift(new Supplier<Action<O, Signal<O>>>() {
			@Override
			public Action<O, Signal<O>> get() {
				return new MaterializeAction<O>();
			}
		});
	}


	/**
	 * Transform the incoming onSubscribe, onNext, onError and onComplete signals into {@link reactor.rx.action.Signal}.
	 * Since the error is materialized as a {@code Signal}, the propagation will be stopped.
	 * Complete signal will first emit a {@code Signal.complete()} and then effectively complete the stream.
	 *
	 * @return {@literal new Stream}
	 */
	@SuppressWarnings("unchecked")
	public final <X> Stream<X> dematerialize() {
		Stream<Signal<X>> thiz = (Stream<Signal<X>>) this;
		return thiz.lift(new Supplier<Action<Signal<X>, X>>() {
			@Override
			public Action<Signal<X>, X> get() {
				return new DematerializeAction<X>();
			}
		});
	}

	/**
	 * Subscribe a new {@link Broadcaster} and return it for future subscribers interactions. Effectively it turns any
	 * stream into an Hot Stream where subscribers will only values from the time T when they subscribe to the returned
	 * stream. Complete and Error signals are however retained unless {@link this#keepAlive()} has been called before.
	 * <p>
	 *
	 * @return a new {@literal stream} whose values are broadcasted to all subscribers
	 */
	public final Stream<O> broadcast() {
		return broadcastOn(getDispatcher());
	}

	/**
	 * Subscribe a new {@link Broadcaster} and return it for future subscribers interactions. Effectively it turns any
	 * stream into an Hot Stream where subscribers will only values from the time T when they subscribe to the returned
	 * stream. Complete and Error signals are however retained unless {@link this#keepAlive()} has been called before.
	 * <p>
	 *
	 * @param dispatcher the dispatcher to run the signals
	 * @return a new {@literal stream} whose values are broadcasted to all subscribers
	 */
	public final Stream<O> broadcastOn(Dispatcher dispatcher) {
		Broadcaster<O> broadcaster = Broadcaster.create(getEnvironment(), dispatcher);
		return broadcastTo(broadcaster);
	}


	/**
	 * Subscribe the passed subscriber, only creating once necessary upstream Subscriptions and returning itself.
	 * Mostly used by other broadcast actions which transform any Stream into a publish-subscribe Stream (every
	 * subscribers
	 * see all values).
	 * <p>
	 *
	 * @param subscriber the subscriber to subscribe to this stream and return
	 * @param <E>        the hydrated generic type for the passed argument, allowing for method chaining
	 * @return {@param subscriber}
	 */
	public final <E extends Subscriber<? super O>> E broadcastTo(E subscriber) {
		subscribe(subscriber);
		return subscriber;
	}

	/**
	 * Create a {@link reactor.fn.support.Tap} that maintains a reference to the last value seen by this {@code
	 * Stream}. The {@link reactor.fn.support.Tap} is
	 * continually updated when new values pass through the {@code Stream}.
	 *
	 * @return the new {@link reactor.fn.support.Tap}
	 * @see Consumer
	 */
	public final TapAndControls<O> tap() {
		final Tap<O> tap = new Tap<>();
		return new TapAndControls<>(tap, consume(tap));
	}

	/**
	 * Create a {@link reactor.fn.support.Tap} that maintains a reference to the last value seen by this {@code
	 * Stream}. The {@link reactor.fn.support.Tap} is
	 * continually updated when new values pass through the {@code Stream}.
	 *
	 * @return the new {@link reactor.fn.support.Tap}
	 * @see Consumer
	 */
	@SuppressWarnings("unchecked")
	public final <E> Stream<E> process(final Processor<O, E> processor) {
		subscribe(processor);
		if(Stream.class.isAssignableFrom(processor.getClass())){
			return (Stream<E>)processor;
		}

		final long capacity = Stream.this.getCapacity();

		return new Stream<E>() {

			@Override
			public long getCapacity() {
				return capacity;
			}

			@Override
			public Environment getEnvironment() {
				return Stream.this.getEnvironment();
			}

			@Override
			public void subscribe(Subscriber<? super E> s) {
				try{
					processor.subscribe(s);
				}catch(Throwable t){
					s.onError(t);
				}
			}
		};
	}

	/**
	 * Defer a Controls operations ready to be requested.
	 *
	 * @return the consuming action
	 */
	public Control consumeLater() {
		return consume(null);
	}

	/**
	 * Instruct the stream to request the produced subscription indefinitely. If the dispatcher
	 * is asynchronous (RingBufferDispatcher for instance), it will proceed the request asynchronously as well.
	 *
	 * @return the consuming action
	 */
	public Control consume() {
		Control controls = consume(null);
		controls.requestMore(Long.MAX_VALUE);
		return controls;
	}

	/**
	 * Instruct the action to request upstream subscription if any for N elements.
	 *
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public Control consume(final long n) {
		Control controls = consume(null);
		if (n > 0) {
			controls.requestMore(n);
		}
		return controls;
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will consume any values accepted by this {@code
	 * Stream}. As such this a terminal action to be placed on a stream flow.
	 * It will also eagerly prefetch upstream publisher.
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link this#observe(reactor.fn.Consumer)}
	 *
	 * @param consumer the consumer to invoke on each value
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public final Control consume(final Consumer<? super O> consumer) {
		return consumeOn(getDispatcher(), consumer);
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will consume any values accepted by this {@code
	 * Stream}. As such this a terminal action to be placed on a stream flow. It will also eagerly prefetch upstream
	 * publisher.
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link this#observe(reactor.fn.Consumer)}
	 *
	 * @param dispatcher the dispatcher to run the consumer
	 * @param consumer   the consumer to invoke on each value
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public final Control consumeOn(Dispatcher dispatcher, final Consumer<? super O> consumer) {
		ConsumerAction<O> consumerAction = new ConsumerAction<O>(dispatcher, consumer, null, null);
		if (dispatcher != getDispatcher()) {
			consumerAction.capacity(getCapacity());
		}
		subscribe(consumerAction);
		return consumerAction;
	}

	/**
	 * Attach 2 {@link Consumer} to this {@code Stream} that will consume any values signaled by this {@code
	 * Stream}. As such this a terminal action to be placed on a stream flow.
	 * Any Error signal will be consumed by the error consumer.
	 * It will also eagerly prefetch upstream publisher.
	 * <p>
	 *
	 * @param consumer      the consumer to invoke on each next signal
	 * @param errorConsumer the consumer to invoke on each error signal
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public final Control consume(final Consumer<? super O> consumer,
	                             Consumer<? super Throwable> errorConsumer) {
		return consumeOn(getDispatcher(), consumer, errorConsumer);
	}

	/**
	 * Attach 2 {@link Consumer} to this {@code Stream} that will consume any values signaled by this {@code
	 * Stream}. As such this a terminal action to be placed on a stream flow.
	 * Any Error signal will be consumed by the error consumer.
	 * It will also eagerly prefetch upstream publisher.
	 * <p>
	 *
	 * @param consumer      the consumer to invoke on each next signal
	 * @param errorConsumer the consumer to invoke on each error signal
	 * @param dispatcher    the dispatcher to run the consumer
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public final Control consumeOn(Dispatcher dispatcher, final Consumer<? super O> consumer,
	                               Consumer<? super Throwable> errorConsumer) {
		return consumeOn(dispatcher, consumer, errorConsumer, null);
	}

	/**
	 * Attach 3 {@link Consumer} to this {@code Stream} that will consume any values signaled by this {@code
	 * Stream}. As such this a terminal action to be placed on a stream flow.
	 * Any Error signal will be consumed by the error consumer.
	 * The Complete signal will be consumed by the complete consumer.
	 * Only error and complete signal will be signaled downstream. It will also eagerly prefetch upstream publisher.
	 * <p>
	 *
	 * @param consumer         the consumer to invoke on each value
	 * @param errorConsumer    the consumer to invoke on each error signal
	 * @param completeConsumer the consumer to invoke on complete signal
	 * @return {@literal new Stream}
	 */
	public final Control consume(final Consumer<? super O> consumer,
	                             Consumer<? super Throwable> errorConsumer,
	                             Consumer<Void> completeConsumer) {
		return consumeOn(getDispatcher(), consumer, errorConsumer, completeConsumer);
	}

	/**
	 * Attach 3 {@link Consumer} to this {@code Stream} that will consume any values signaled by this {@code
	 * Stream}. As such this a terminal action to be placed on a stream flow.
	 * Any Error signal will be consumed by the error consumer.
	 * The Complete signal will be consumed by the complete consumer.
	 * Only error and complete signal will be signaled downstream. It will also eagerly prefetch upstream publisher.
	 * <p>
	 *
	 * @param consumer         the consumer to invoke on each value
	 * @param errorConsumer    the consumer to invoke on each error signal
	 * @param completeConsumer the consumer to invoke on complete signal
	 * @param dispatcher       the dispatcher to run the consumer
	 * @return {@literal new Stream}
	 */
	public final Control consumeOn(Dispatcher dispatcher, final Consumer<? super O> consumer,
	                               Consumer<? super Throwable> errorConsumer,
	                               Consumer<Void> completeConsumer) {
		ConsumerAction<O> consumerAction =
				new ConsumerAction<O>(dispatcher, consumer, errorConsumer, completeConsumer);

		if (dispatcher != getDispatcher()) {
			consumerAction.capacity(getCapacity());
		}

		subscribe(consumerAction);
		return consumerAction;
	}


	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will consume any values accepted by this {@code
	 * Stream}. As such this a terminal action to be placed on a stream flow. It will also eagerly prefetch upstream
	 * publisher.
	 * <p>
	 * The passed {code requestMapper} function will receive the {@link Stream} of the last N requested elements
	 * -starting with the
	 * capacity defined for the stream- when the N elements have been consumed. It will return a {@link Publisher} of
	 * long signals
	 * S that will instruct the consumer to request S more elements, possibly altering the "batch" size if wished.
	 * <p>
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link this#observe(reactor.fn.Consumer)}
	 *
	 * @param consumer the consumer to invoke on each value
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public final Control batchConsume(final Consumer<? super O> consumer,
	                                  final Function<Long, ? extends Long> requestMapper) {
		return batchConsumeOn(getDispatcher(), consumer, requestMapper);
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will consume any values accepted by this {@code
	 * Stream}. As such this a terminal action to be placed on a stream flow. It will also eagerly prefetch upstream
	 * publisher.
	 * <p>
	 * The passed {code requestMapper} function will receive the {@link Stream} of the last N requested elements
	 * -starting with the
	 * capacity defined for the stream- when the N elements have been consumed. It will return a {@link Publisher} of
	 * long signals
	 * S that will instruct the consumer to request S more elements.
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link this#observe(reactor.fn.Consumer)}
	 *
	 * @param consumer the consumer to invoke on each value
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public final Control adaptiveConsume(final Consumer<? super O> consumer,
	                                     final Function<Stream<Long>, ? extends Publisher<? extends Long>>
			                                     requestMapper) {
		return adaptiveConsumeOn(getDispatcher(), consumer, requestMapper);
	}


	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will consume any values accepted by this {@code
	 * Stream}. As such this a terminal action to be placed on a stream flow. It will also eagerly prefetch upstream
	 * publisher.
	 * <p>
	 * The passed {code requestMapper} function will receive the {@link Stream} of the last N requested elements
	 * -starting with the
	 * capacity defined for the stream- when the N elements have been consumed. It will return a {@link Publisher} of
	 * long signals
	 * S that will instruct the consumer to request S more elements, possibly altering the "batch" size if wished.
	 * <p>
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link this#observe(reactor.fn.Consumer)}
	 *
	 * @param consumer the consumer to invoke on each value
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public final Control batchConsumeOn(final Dispatcher dispatcher,
	                                    final Consumer<? super O> consumer,
	                                    final Function<Long, ? extends Long>
			                                    requestMapper) {
		return adaptiveConsumeOn(dispatcher, consumer, new Function<Stream<Long>, Publisher<? extends Long>>() {
			@Override
			public Publisher<? extends Long> apply(Stream<Long> longStream) {
				return longStream.map(requestMapper);
			}
		});
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will consume any values accepted by this {@code
	 * Stream}. As such this a terminal action to be placed on a stream flow. It will also eagerly prefetch upstream
	 * publisher.
	 * <p>
	 * The passed {code requestMapper} function will receive the {@link Stream} of the last N requested elements
	 * -starting with the
	 * capacity defined for the stream- when the N elements have been consumed. It will return a {@link Publisher} of
	 * long signals
	 * S that will instruct the consumer to request S more elements.
	 * <p>
	 * Multiple long signals S can be requested before a given request complete and therefore
	 * an approriate ordering Dispatcher should be used.
	 * <p>
	 * <p>
	 * For a passive version that observe and forward incoming data see {@link this#observe(reactor.fn.Consumer)}
	 *
	 * @param consumer the consumer to invoke on each value
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public final Control adaptiveConsumeOn(final Dispatcher dispatcher,
	                                       final Consumer<? super O> consumer,
	                                       final Function<Stream<Long>, ? extends Publisher<? extends Long>>
			                                       requestMapper) {
		AdaptiveConsumerAction<O> consumerAction =
				new AdaptiveConsumerAction<O>(dispatcher, getCapacity(), consumer, requestMapper);

		subscribe(consumerAction);
		if (consumer != null) {
			consumerAction.requestMore(consumerAction.getCapacity());
		}
		return consumerAction;
	}

	/**
	 * Assign a new Environment and its default Dispatcher to the returned Stream. If the dispatcher is different,
	 * the new action will take
	 * care of buffering incoming data into a StreamSubscription. Otherwise default behavior is picked:
	 * FireHose synchronous subscription is the parent stream != null
	 *
	 * @param environment the environment to get dispatcher from {@link reactor.Environment#getDefaultDispatcher()}
	 * @return a new {@link Stream} running on a different {@link Dispatcher}
	 */
	public final Stream<O> dispatchOn(@Nonnull final Environment environment) {
		return dispatchOn(environment, environment.getDefaultDispatcher());
	}

	/**
	 * Assign a new Dispatcher to handle upstream request to the returned Stream.
	 *
	 * @param environment the environment to get dispatcher from {@link reactor.Environment#getDefaultDispatcher()}
	 * @return a new {@link Stream} whose requests are running on a different {@link Dispatcher}
	 */
	public final Stream<O> subscribeOn(@Nonnull final Environment environment) {
		return subscribeOn(environment.getDefaultDispatcher());
	}

	/**
	 * Assign a new Dispatcher to the returned Stream. If the dispatcher is different, the new action will take
	 * care of buffering incoming data into a StreamSubscription. Otherwise default behavior is picked:
	 * FireHose synchronous subscription is the parent stream != null
	 *
	 * @param dispatcher the new dispatcher
	 * @return a new {@link Stream} running on a different {@link Dispatcher}
	 */
	public final Stream<O> dispatchOn(@Nonnull final Dispatcher dispatcher) {
		return dispatchOn(null, dispatcher);
	}


	/**
	 * Assign a new Dispatcher to handle upstream request to the returned Stream.
	 *
	 * @param sub the subscriber to request using the current dispatcher
	 * @param currentDispatcher the new dispatcher
	 */
	public final void subscribeOn(@Nonnull final Dispatcher currentDispatcher, Subscriber<? super O> sub) {
		subscribeOn(currentDispatcher).subscribe(sub);
	}
	/**
	 * Assign a new Dispatcher to handle upstream request to the returned Stream.
	 *
	 * @param currentDispatcher the new dispatcher
	 * @return a new {@link Stream} whose requests are running on a different {@link Dispatcher}
	 */
	public final Stream<O> subscribeOn(@Nonnull final Dispatcher currentDispatcher) {
		return new Stream<O>() {
			@Override
			public long getCapacity() {
				return Stream.this.getCapacity();
			}

			@Override
			public Environment getEnvironment() {
				return Stream.this.getEnvironment();
			}

			@Override
			public Dispatcher getDispatcher() {
				return currentDispatcher;
			}

			@Override
			public void subscribe(Subscriber<? super O> s) {
				currentDispatcher.dispatch(s, new Consumer<Subscriber<? super O>>() {
					@Override
					public void accept(Subscriber<? super O> subscriber) {
						Stream.this.subscribe(subscriber);
					}
				}, null);
			}

		};
	}

	/**
	 * Assign the a new Dispatcher and an Environment to the returned Stream. If the dispatcher is different,
	 * the new action will take
	 * care of buffering incoming data into a StreamSubscription. Otherwise default behavior is picked:
	 * FireHose synchronous subscription is the parent stream != null
	 *
	 * @param dispatcher  the new dispatcher
	 * @param environment the environment
	 * @return a new {@link Stream} running on a different {@link Dispatcher}
	 */
	public Stream<O> dispatchOn(final Environment environment, @Nonnull final Dispatcher dispatcher) {
		if (dispatcher == SynchronousDispatcher.INSTANCE
				|| dispatcher == getDispatcher()) {

			if (environment != null && environment != getEnvironment()) {
				return env(environment);
			} else {
				return this;
			}
		}

		Assert.state(dispatcher.supportsOrdering(), "Dispatcher provided doesn't support event ordering. " +
				" For concurrent signal dispatching, refer to #partition()/groupBy() method and assign individual single " +
				"dispatchers. ");

		long _capacity = Action.evaluateCapacity(dispatcher.backlogSize());
		long parentCapacity = getCapacity();
		final Dispatcher parentDispatcher = getDispatcher();

		final long capacity = _capacity > parentCapacity ? parentCapacity : _capacity;

		return new LiftStream<O, O>(this, new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new DispatcherAction<O>(dispatcher, parentDispatcher).capacity(capacity);
			}
		}) {
			@Override
			public Dispatcher getDispatcher() {
				return dispatcher;
			}

			@Override
			public Environment getEnvironment() {
				return environment;
			}

			@Override
			public long getCapacity() {
				return capacity;
			}
		};
	}


	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will observe any values accepted by this {@code
	 * Stream}.
	 *
	 * @param consumer the consumer to invoke on each value
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final Stream<O> observe(@Nonnull final Consumer<? super O> consumer) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new CallbackAction<O>(consumer, null);
			}
		});
	}

	/**
	 * Cache all signal to this {@code Stream} and release them on request that will observe any values accepted by this
	 * {@code
	 * Stream}.
	 *
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final Stream<O> cache() {
		Action<O, O> cacheAction = new CacheAction<O>();
		subscribe(cacheAction);
		return cacheAction;
	}


	/**
	 * Attach a {@link java.util.logging.Logger} to this {@code Stream} that will observe any signal emitted.
	 *
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final Stream<O> log() {
		return log(null);
	}

	/**
	 * Attach a {@link java.util.logging.Logger} to this {@code Stream} that will observe any signal emitted.
	 *
	 * @param name The logger name
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final Stream<O> log(final String name) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new LoggerAction<O>(name);
			}
		});
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will observe any complete signal
	 *
	 * @param consumer the consumer to invoke on complete
	 * @return {@literal a new stream}
	 * @since 2.0
	 */
	public final Stream<O> observeComplete(@Nonnull final Consumer<Void> consumer) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new CallbackAction<O>(null, consumer);
			}
		});
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will observe any subscribe signal
	 *
	 * @param consumer the consumer to invoke ont subscribe
	 * @return {@literal a new stream}
	 * @since 2.0
	 */
	public final Stream<O> observeSubscribe(@Nonnull final Consumer<? super Subscriber<? super O>> consumer) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new StreamStateCallbackAction<O>(consumer, null);
			}
		});
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will observe any cancel signal
	 *
	 * @param consumer the consumer to invoke on cancel
	 * @return {@literal a new stream}
	 * @since 2.0
	 */
	public final Stream<O> observeCancel(@Nonnull final Consumer<Void> consumer) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new StreamStateCallbackAction<O>(null, consumer);
			}
		});
	}

	/**
	 * Connect an error-proof action that will transform an incoming error signal into a complete signal.
	 *
	 * @return a new fail-proof {@link Stream}
	 */
	public Stream<O> ignoreError() {
		return ignoreError(Predicates.always());
	}

	/**
	 * Connect an error-proof action based on the given predicate matching the current error.
	 *
	 * @param ignorePredicate a predicate to test if an error should be transformed to a complete signal.
	 * @return a new fail-proof {@link Stream}
	 */
	public <E> Stream<O> ignoreError(final Predicate<? super Throwable> ignorePredicate) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new IgnoreErrorAction<O>(ignorePredicate);
			}
		});
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will observe terminal signal complete|error.
	 * The consumer will listen for the signal and introspect its state.
	 *
	 * @param consumer the consumer to invoke on terminal signal
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final Stream<O> finallyDo(final Consumer<Signal<O>> consumer) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new FinallyAction<O>(consumer);
			}
		});
	}

	/**
	 * Create an operation that returns the passed value if the Stream has completed without any emitted signals.
	 *
	 * @param defaultValue the value to forward if the stream is empty
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final Stream<O> defaultIfEmpty(final O defaultValue) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new DefaultIfEmptyAction<O>(defaultValue);
			}
		});
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code V} and pass it into
	 * another {@code Stream}.
	 *
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@link Stream} containing the transformed values
	 */
	public final <V> Stream<V> map(@Nonnull final Function<? super O, ? extends V> fn) {
		return lift(new Supplier<Action<O, V>>() {
			@Override
			public Action<O, V> get() {
				return new MapAction<O, V>(fn);
			}
		});
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code Stream<O,V>} and pass
	 * it into another {@code Stream}.
	 *
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@link Stream} containing the transformed values
	 * @since 1.1, 2.0
	 */
	public final <V> Stream<V> flatMap(@Nonnull final Function<? super O,
			? extends Publisher<? extends V>> fn) {
		return map(fn).merge();
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code Stream<O,V>} and pass
	 * it into another {@code Stream}. The produced stream will emit the data from the most recent transformed stream.
	 *
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@link Stream} containing the transformed values
	 * @since 1.1, 2.0
	 */
	public final <V> Stream<V> switchMap(@Nonnull final Function<? super O,
			Publisher<? extends V>> fn) {
		return map(fn).lift(new Supplier<Action<Publisher<? extends V>, V>>() {
			@Override
			public Action<Publisher<? extends V>, V> get() {
				return new SwitchAction<V>(getDispatcher());
			}
		});
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code Stream<O,V>} and pass
	 * it into another {@code Stream}. The produced stream will emit the data from all transformed streams in order.
	 *
	 * @param fn  the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@link Stream} containing the transformed values
	 * @since 1.1, 2.0
	 */
	public final <V> Stream<V> concatMap(@Nonnull final Function<? super O,
			Publisher<? extends V>> fn) {
		return map(fn).lift(new Supplier<Action<Publisher<? extends V>, V>>() {
			@Override
			public Action<Publisher<? extends V>, V> get() {
				return new ConcatAction<V>();
			}
		});
	}

	/**
	 * {@link this#lift(Supplier)} all the nested {@link Publisher} values to a new {@link Stream}.
	 * Dynamic merge requires use of reactive-pull
	 * offered by default StreamSubscription. If merge hasn't getCapacity() to take new elements because its {@link
	 * this#getCapacity()(long)} instructed so, the subscription will buffer
	 * them.
	 *
	 * @param <V> the inner stream flowing data type that will be the produced signal.
	 * @return the merged stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <V> Stream<V> merge() {
		return fanIn(null);
	}

	/**
	 * {@link this#lift(Supplier)} all the nested {@link Publisher} values from this current upstream and from the
	 * passed publisher.
	 *
	 * @return the merged stream
	 * @since 2.0
	 */
	public final Stream<O> mergeWith(final Publisher<? extends O> publisher) {
		return new Stream<O>() {
			@Override
			public void subscribe(Subscriber<? super O> s) {
				new MergeAction<>(SynchronousDispatcher.INSTANCE, Arrays.asList(Stream.this, publisher))
						.subscribe(s);
			}

			@Override
			public Environment getEnvironment() {
				return Stream.this.getEnvironment();
			}

			@Override
			public long getCapacity() {
				return Stream.this.getCapacity();
			}

			@Override
			public Dispatcher getDispatcher() {
				return Stream.this.getDispatcher();
			}
		};
	}

	/**
	 * {@link this#lift(Supplier)} all the nested {@link Publisher} values from this current upstream and then on
	 * complete consume from the
	 * passed publisher.
	 *
	 * @return the merged stream
	 * @since 2.0
	 */
	public final Stream<O> concatWith(final Publisher<? extends O> publisher) {
		return new Stream<O>() {
			@Override
			public void subscribe(Subscriber<? super O> s) {
				Stream<Publisher<? extends O>> just = Streams.just(Stream.this, publisher);
				ConcatAction<O> concatBAction = new ConcatAction<>();
				just.subscribe(concatBAction);
				concatBAction.subscribe(s);
			}

			@Override
			public long getCapacity() {
				return Stream.this.getCapacity();
			}

			@Override
			public Dispatcher getDispatcher() {
				return Stream.this.getDispatcher();
			}

			@Override
			public Environment getEnvironment() {
				return Stream.this.getEnvironment();
			}
		};
	}

	/**
	 * Start emitting all items from the passed publisher then emits from the current stream.
	 *
	 * @return the merged stream
	 * @since 2.0
	 */
	public final Stream<O> startWith(final Iterable<O> iterable) {
		return startWith(Streams.from(iterable));
	}

	/**
	 * Start emitting all items from the passed publisher then emits from the current stream.
	 *
	 * @return the merged stream
	 * @since 2.0
	 */
	public final Stream<O> startWith(final O value) {
		return startWith(Streams.just(value));
	}

	/**
	 * Start emitting all items from the passed publisher then emits from the current stream.
	 *
	 * @return the merged stream
	 * @since 2.0
	 */
	public final Stream<O> startWith(final Publisher<? extends O> publisher) {
		if(publisher == null) return this;
		return Streams.concat(publisher, this);
	}

	/**
	 * {@link this#lift(Supplier)} all the nested {@link Publisher} values to a new {@link Stream} until one of them
	 * complete.
	 * The result will be produced with a list of each upstream most recent emitted data.
	 *
	 * @return the zipped and joined stream
	 * @since 2.0
	 */
	public final <V> Stream<List<V>> join() {
		return zip(ZipAction.<TupleN, V>joinZipper());
	}

	/**
	 * {@link this#lift(Supplier)} all the nested {@link Publisher} values to a new {@link Stream} until one of them
	 * complete.
	 * The result will be produced with a list of each upstream most recent emitted data.
	 *
	 * @return the zipped and joined stream
	 * @since 2.0
	 */
	public final <V> Stream<List<V>> joinWith(Publisher<? extends V> publisher) {
		return zipWith(publisher, ZipAction.<Tuple2<O, V>, V>joinZipper());
	}


	/**
	 * {@link this#lift(Supplier)} all the nested {@link Publisher} values to a new {@link Stream} until one of them
	 * complete.
	 * The result will be produced by the zipper transformation from a tuple of each upstream most recent emitted data.
	 *
	 * @return the merged stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <V> Stream<V> zip(final @Nonnull Function<TupleN, ? extends V> zipper) {
		final Stream<Publisher<?>> thiz = (Stream<Publisher<?>>) this;

		return thiz.lift(new Supplier<Action<Publisher<?>, V>>() {
			@Override
			public Action<Publisher<?>, V> get() {
				return new DynamicMergeAction<Object, V>(
						new ZipAction<Object, V, TupleN>(SynchronousDispatcher.INSTANCE, zipper, null)).
						capacity(getCapacity());
			}
		});
	}

	/**
	 * {@link this#lift(Supplier)} all the nested {@link Publisher} values to a new {@link Stream} until one of them
	 * complete.
	 * The result will be produced by the zipper transformation from a tuple of each upstream most recent emitted data.
	 *
	 * @return the zipped stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <T2, V> Stream<V> zipWith(Iterable<? extends T2> iterable,
	                                       @Nonnull Function<Tuple2<O, T2>, V> zipper) {
		return zipWith(Streams.from(iterable), zipper);
	}

	/**
	 * {@link this#lift(Supplier)} with the passed {@link Publisher} values to a new {@link Stream} until one of them
	 * complete.
	 * The result will be produced by the zipper transformation from a tuple of each upstream most recent emitted data.
	 *
	 * @return the zipped stream
	 * @since 2.0
	 */
	public final <T2, V> Stream<V> zipWith(final Publisher<? extends T2> publisher,
	                                       final @Nonnull Function<Tuple2<O, T2>, V> zipper) {
		return new Stream<V>() {
			@Override
			public void subscribe(Subscriber<? super V> s) {
				new ZipAction<>(SynchronousDispatcher.INSTANCE, zipper, Arrays.asList(Stream.this, publisher))
						.subscribe(s);
			}

			@Override
			public long getCapacity() {
				return Stream.this.getCapacity();
			}

			@Override
			public Dispatcher getDispatcher() {
				return Stream.this.getDispatcher();
			}

			@Override
			public Environment getEnvironment() {
				return Stream.this.getEnvironment();
			}
		};
	}

	/**
	 * {@link this#lift(Supplier)} all the nested {@link Publisher} values to a new {@link Stream} calling the logic
	 * inside the provided fanInAction for complex merging strategies.
	 * {@link reactor.rx.action.combination.FanInAction} provides helpers to create subscriber for each source,
	 * a registry of incoming sources and overriding doXXX signals as usual to produce the result via
	 * reactor.rx.action.Action#broadcastXXX.
	 * <p>
	 * A default fanInAction will act like {@link this#merge()}, passing values to doNext. In java8 one can then
	 * implement
	 * stream.fanIn(data -> broadcastNext(data)) or stream.fanIn(System.out::println)
	 * <p>
	 * Dynamic merge (moving nested data into the top-level returned stream) requires use of reactive-pull offered by
	 * default StreamSubscription. If merge hasn't getCapacity() to
	 * take new elements because its {@link
	 * this#getCapacity()(long)} instructed so, the subscription will buffer
	 * them.
	 *
	 * @param <T> the nested type of flowing upstream Stream.
	 * @param <V> the produced output
	 * @return the zipped stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public <T, V> Stream<V> fanIn(
			final FanInAction<T, ?, V, ? extends FanInAction.InnerSubscriber<T, ?, V>> fanInAction
	) {
		final Stream<Publisher<? extends T>> thiz = (Stream<Publisher<? extends T>>) this;

		return thiz.lift(new Supplier<Action<Publisher<? extends T>, V>>() {
			@Override
			public Action<Publisher<? extends T>, V> get() {
				return new DynamicMergeAction<T, V>(fanInAction).
						capacity(getCapacity());
			}
		});
	}

	/**
	 * Bind the stream to a given {@param elements} volume of in-flight data:
	 * - An {@link Action} will request up to the defined volume upstream.
	 * - An {@link Action} will track the pending requests and fire up to {@param elements} when the previous volume has
	 * been processed.
	 * - A {@link reactor.rx.action.aggregation.BatchAction} and any other size-bound action will be limited to the
	 * defined volume.
	 * <p>
	 * <p>
	 * A stream capacity can't be superior to the underlying dispatcher capacity: if the {@param elements} overflow the
	 * dispatcher backlog size, the capacity will be aligned automatically to fit it.
	 * RingBufferDispatcher will for instance take to a power of 2 size up to {@literal Integer.MAX_VALUE},
	 * where a Stream can be sized up to {@literal Long.MAX_VALUE} in flight data.
	 * <p>
	 * <p>
	 * When the stream receives more elements than requested, incoming data is eventually staged in a {@link
	 * org.reactivestreams.Subscription}.
	 * The subscription can react differently according to the implementation in-use,
	 * the default strategy is as following:
	 * - The first-level of pair compositions Stream->Action will overflow data in a {@link reactor.core.queue
	 * .CompletableQueue},
	 * ready to be polled when the action fire the pending requests.
	 * - The following pairs of Action->Action will synchronously pass data
	 * - Any pair of Stream->Subscriber or Action->Subscriber will behave as with the root Stream->Action pair rule.
	 * - {@link this#onOverflowBuffer()} force this staging behavior, with a possibilty to pass a {@link reactor.core.queue
	 * .PersistentQueue}
	 *
	 * @param elements maximum number of in-flight data
	 * @return a backpressure capable stream
	 */
	public Stream<O> capacity(final long elements) {
		if(elements == getCapacity()) return this;

		return new Stream<O>() {
			@Override
			public void subscribe(Subscriber<? super O> s) {
				Stream.this.subscribe(s);
			}

			@Override
			public Dispatcher getDispatcher() {
				return Stream.this.getDispatcher();
			}

			@Override
			public Environment getEnvironment() {
				return Stream.this.getEnvironment();
			}

			@Override
			public long getCapacity() {
				return elements;
			}
		};
	}

	/**
	 * Attach a No-Op Action that only serves the purpose of buffering incoming values if not enough demand is signaled
	 * downstream. A buffering capable stream will prevent underlying dispatcher to be saturated (and sometimes
	 * blocking).
	 *
	 * @return a buffered stream
	 * @since 2.0
	 */
	public final Stream<O> onOverflowBuffer() {
		return onOverflowBuffer(new Supplier<CompletableQueue<O>>() {
			@Override
			public CompletableQueue<O> get() {
				return new CompletableLinkedQueue<O>();
			}
		});
	}

	/**
	 * Attach a No-Op Action that only serves the purpose of buffering incoming values if not enough demand is signaled
	 * downstream. A buffering capable stream will prevent underlying dispatcher to be saturated (and sometimes
	 * blocking).
	 *
	 * @param queueSupplier A completable queue {@link reactor.fn.Supplier} to provide support for overflow
	 * @return a buffered stream
	 * @since 2.0
	 */
	public Stream<O> onOverflowBuffer(final Supplier<? extends CompletableQueue<O>> queueSupplier) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new FlowControlAction<O>(queueSupplier);
			}
		});
	}

	/**
	 * Attach a No-Op Action that only serves the purpose of dropping incoming values if not enough demand is signaled
	 * downstream. A dropping stream will prevent underlying dispatcher to be saturated (and sometimes
	 * blocking).
	 *
	 * @return a dropping stream
	 * @since 2.0
	 */
	public final Stream<O> onOverflowDrop() {
		return onOverflowBuffer(null);
	}

	/**
	 * Evaluate each accepted value against the given {@link Predicate}. If the predicate test succeeds, the value is
	 * passed into the new {@code Stream}. If the predicate test fails, the value is ignored.
	 *
	 * @param p the {@link Predicate} to test values against
	 * @return a new {@link Stream} containing only values that pass the predicate test
	 */
	public final Stream<O> filter(final Predicate<? super O> p) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new FilterAction<O>(p);
			}
		});
	}

	/**
	 * Evaluate each accepted boolean value. If the predicate test succeeds, the value is
	 * passed into the new {@code Stream}. If the predicate test fails, the value is ignored.
	 *
	 * @return a new {@link Stream} containing only values that pass the predicate test
	 * @since 1.1, 2.0
	 */
	@SuppressWarnings("unchecked")
	public final Stream<Boolean> filter() {
		return ((Stream<Boolean>) this).filter(FilterAction.simplePredicate);
	}


	/**
	 * Create a new {@code Stream} whose only value will be the current instance of the {@link Stream}.
	 *
	 * @return a new {@link Stream} whose only value will be the materialized current {@link Stream}
	 * @since 2.0
	 */
	public final Stream<Stream<O>> nest() {
		return Streams.just(this);
	}


	/**
	 * Create a new {@code Stream} which will re-subscribe its oldest parent-child stream pair. The action will start
	 * propagating errors after {@literal Integer.MAX_VALUE}.
	 *
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> retry() {
		return retry(-1);
	}

	/**
	 * Create a new {@code Stream} which will re-subscribe its oldest parent-child stream pair. The action will start
	 * propagating errors after {@param numRetries}.
	 * This is generally useful for retry strategies and fault-tolerant streams.
	 *
	 * @param numRetries the number of times to tolerate an error
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> retry(int numRetries) {
		return retry(numRetries, null);
	}

	/**
	 * Create a new {@code Stream} which will re-subscribe its oldest parent-child stream pair.
	 * {@param retryMatcher} will test an incoming {@link Throwable}, if positive the retry will occur.
	 * This is generally useful for retry strategies and fault-tolerant streams.
	 *
	 * @param retryMatcher the predicate to evaluate if retry should occur based on a given error signal
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> retry(Predicate<Throwable> retryMatcher) {
		return retry(-1, retryMatcher);
	}

	/**
	 * Create a new {@code Stream} which will re-subscribe its oldest parent-child stream pair. The action will start
	 * propagating errors after {@param numRetries}. {@param retryMatcher} will test an incoming {@Throwable},
	 * if positive
	 * the retry will occur (in conjonction with the {@param numRetries} condition).
	 * This is generally useful for retry strategies and fault-tolerant streams.
	 *
	 * @param numRetries   the number of times to tolerate an error
	 * @param retryMatcher the predicate to evaluate if retry should occur based on a given error signal
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> retry(final int numRetries, final Predicate<Throwable> retryMatcher) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new RetryAction<O>(getDispatcher(), numRetries, retryMatcher, Stream.this);
			}
		});
	}

	/**
	 * Create a new {@code Stream} which will re-subscribe its oldest parent-child stream pair if the exception is of
	 * the given type.
	 * The recoveredValues subscriber will be emitted the associated value if any. If it doesn't match the given
	 * exception type, the error signal will be propagated downstream but not to the recovered values sink.
	 *
	 * @param recoveredValuesSink the subscriber to listen for recovered values
	 * @param exceptionType       the type of exceptions to handle
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> recover(@Nonnull final Class<? extends Throwable> exceptionType,
	                               final Subscriber<Object> recoveredValuesSink) {
		return retryWhen(new Function<Stream<? extends Throwable>, Publisher<?>>() {

			@Override
			public Publisher<?> apply(Stream<? extends Throwable> stream) {

				stream.map(new Function<Throwable, Object>() {
					@Override
					public Object apply(Throwable throwable) {
						if (exceptionType.isAssignableFrom(throwable.getClass())) {
							return Exceptions.getFinalValueCause(throwable);
						} else {
							return null;
						}
					}
				}).subscribe(recoveredValuesSink);

				return stream.map(new Function<Throwable, Signal<Throwable>>() {

					@Override
					public Signal<Throwable> apply(Throwable throwable) {
						if (exceptionType.isAssignableFrom(throwable.getClass())) {
							return Signal.next(throwable);
						} else {
							return Signal.<Throwable>error(throwable);
						}
					}
				}).<Throwable>dematerialize();
			}
		});

	}


	/**
	 * Create a new {@code Stream} which will re-subscribe its oldest parent-child stream pair if the backOff stream
	 * produced by the passed mapper emits any next data or complete signal. It will propagate the error if the backOff
	 * stream emits an error signal.
	 *
	 * @param backOffStream the function taking the error stream as an input and returning a new stream that applies
	 *                      some backoff policy e.g. Streams.timer
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> retryWhen(final Function<? super Stream<? extends Throwable>, ? extends Publisher<?>>
			                                 backOffStream) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new RetryWhenAction<O>(getDispatcher(), backOffStream, Stream.this);
			}
		});
	}

	/**
	 * Create a new {@code Stream} which will keep re-subscribing its oldest parent-child stream pair on complete.
	 *
	 * @return a new infinitely repeated {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> repeat() {
		return repeat(-1);
	}

	/**
	 * Create a new {@code Stream} which will keep re-subscribing its oldest parent-child stream pair on complete.
	 * The action will be propagating complete after {@param numRepeat}.
	 * if positive
	 *
	 * @param numRepeat the number of times to re-subscribe on complete
	 * @return a new repeated {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> repeat(final int numRepeat) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new RepeatAction<O>(getDispatcher(), numRepeat, Stream.this);
			}
		});
	}


	/**
	 * Create a new {@code Stream} which will re-subscribe its oldest parent-child stream pair if the backOff stream
	 * produced by the passed mapper emits any next signal. It will propagate the complete and error if the backoff
	 * stream emits the relative signals.
	 *
	 * @param backOffStream the function taking a stream of complete timestamp in millis as an input and returning a new
	 *                      stream that applies some backoff policy, e.g. @{link Streams#timer(long)}
	 * @return a new repeated {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> repeatWhen(final Function<? super Stream<? extends Long>, ? extends Publisher<?>>
			                                  backOffStream) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new RepeatWhenAction<O>(getDispatcher(), backOffStream, Stream.this);
			}
		});
	}

	/**
	 * Create a new {@code Stream} that will signal the last element observed before complete signal.
	 *
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> last() {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new LastAction<O>();
			}
		});
	}

	/**
	 * Create a new {@code Stream} that will signal next elements up to {@param max} times.
	 *
	 * @param max the number of times to broadcast next signals before completing
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> take(final long max) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new TakeAction<O>(max);
			}
		});
	}

	/**
	 * Create a new {@code Stream} that will signal next elements up to the specified {@param time}.
	 *
	 * @param time the time window to broadcast next signals before completing
	 * @param unit the time unit to use
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> take(long time, TimeUnit unit) {
		return take(time, unit, getTimer());
	}

	/**
	 * Create a new {@code Stream} that will signal next elements up to the specified {@param time}.
	 *
	 * @param time  the time window to broadcast next signals before completing
	 * @param unit  the time unit to use
	 * @param timer the Timer to use
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> take(final long time, final TimeUnit unit, final Timer timer) {
		if (time > 0) {
			Assert.isTrue(timer != null, "Timer can't be found, try assigning an environment to the stream");
			return lift(new Supplier<Action<O, O>>() {
				@Override
				public Action<O, O> get() {
					return new TakeUntilTimeout<O>(getDispatcher(), time, unit, timer);
				}
			});
		} else {
			return Streams.empty();
		}
	}

	/**
	 * Create a new {@code Stream} that will signal next elements while {@param limitMatcher} is true.
	 *
	 * @param limitMatcher the predicate to evaluate for starting dropping events and completing
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> takeWhile(final Predicate<O> limitMatcher) {
			return lift(new Supplier<Action<O, O>>() {
				@Override
				public Action<O, O> get() {
					return new TakeWhileAction<O>(limitMatcher);
				}
			});
	}

	/**
	 * Create a new {@code Stream} that will NOT signal next elements up to {@param max} times.
	 *
	 * @param max the number of times to drop next signals before starting
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> skip(long max) {
		return skipWhile(max, null);
	}

	/**
	 * Create a new {@code Stream} that will NOT signal next elements up to the specified {@param time}.
	 *
	 * @param time the time window to drop next signals before starting
	 * @param unit the time unit to use
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> skip(long time, TimeUnit unit) {
		return skip(time, unit, getTimer());
	}

	/**
	 * Create a new {@code Stream} that will NOT signal next elements up to the specified {@param time}.
	 *
	 * @param time  the time window to drop next signals before starting
	 * @param unit  the time unit to use
	 * @param timer the Timer to use
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> skip(final long time, final TimeUnit unit, final Timer timer) {
		if (time > 0) {
			Assert.isTrue(timer != null, "Timer can't be found, try assigning an environment to the stream");
			return lift(new Supplier<Action<O, O>>() {
				@Override
				public Action<O, O> get() {
					return new SkipUntilTimeout<O>(time, unit, timer);
				}
			});
		} else {
			return this;
		}
	}

	/**
	 * Create a new {@code Stream} that will NOT signal next elements while {@param limitMatcher} is true.
	 *
	 * @param limitMatcher the predicate to evaluate to start broadcasting events
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> skipWhile(Predicate<O> limitMatcher) {
		return skipWhile(Long.MAX_VALUE, limitMatcher);
	}

	/**
	 * Create a new {@code Stream} that will NOT signal next elements while {@param limitMatcher} is true or
	 * up to {@param max} times.
	 *
	 * @param max          the number of times to drop next signals before starting
	 * @param limitMatcher the predicate to evaluate for starting dropping events and completing
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> skipWhile(final long max, final Predicate<O> limitMatcher) {
		if (max > 0) {
			return lift(new Supplier<Action<O, O>>() {
				@Override
				public Action<O, O> get() {
					return new SkipAction<O>(limitMatcher, max);
				}
			});
		} else {
			return this;
		}
	}

	/**
	 * Create a new {@code Stream} that accepts a {@link reactor.fn.tuple.Tuple2} of T1 {@link Long} system time in
	 * millis and T2 {@link <T>} associated data
	 *
	 * @return a new {@link Stream} that emits tuples of millis time and matching data
	 * @since 2.0
	 */
	public final Stream<Tuple2<Long, O>> timestamp() {
		return lift(new Supplier<Action<O, Tuple2<Long, O>>>() {
			@Override
			public Action<O, Tuple2<Long, O>> get() {
				return new TimestampAction<O>();
			}
		});
	}

	/**
	 * Create a new {@code Stream} that accepts a {@link reactor.fn.tuple.Tuple2} of T1 {@link Long} timemillis and T2
	 * {@link <T>} associated data. The timemillis corresponds to the elapsed time between the subscribe and the first
	 * next signal OR between two next signals.
	 *
	 * @return a new {@link Stream} that emits tuples of time elapsed in milliseconds and matching data
	 * @since 2.0
	 */
	public final Stream<Tuple2<Long, O>> elapsed() {
		return lift(new Supplier<Action<O, Tuple2<Long, O>>>() {
			@Override
			public Action<O, Tuple2<Long, O>> get() {
				return new ElapsedAction<O>();
			}
		});
	}

	/**
	 * Create a new {@code Stream} that emits an item at a specified index from a source {@code Stream}
	 *
	 * @param index index of an item
	 * @return a source item at a specified index
	 */
	public final Stream<O> elementAt(final int index) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new ElementAtAction<O>(index);
			}
		});
	}

	/**
	 * Create a new {@code Stream} that emits an item at a specified index from a source {@code Stream}
	 * or default value when index is out of bounds
	 *
	 * @param index index of an item
	 * @return a source item at a specified index or a default value
	 */
	public final Stream<O> elementAtOrDefault(final int index, final O defaultValue) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new ElementAtAction<O>(index, defaultValue);
			}
		});
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch. Requires a {@code
	 * getCapacity()} to have been set.
	 * <p>
	 * When a new batch is triggered, the first value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @return a new {@link Stream} whose values are the first value of each batch
	 */
	public final Stream<O> sampleFirst() {
		return sampleFirst((int) Math.min(Integer.MAX_VALUE, getCapacity()));
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch.
	 * <p>
	 * When a new batch is triggered, the first value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @param batchSize the batch size to use
	 * @return a new {@link Stream} whose values are the first value of each batch)
	 */
	public final Stream<O> sampleFirst(final int batchSize) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new SampleAction<O>(getDispatcher(), batchSize, true);
			}
		});
	}


	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch.
	 *
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @return a new {@link Stream} whose values are the first value of each batch
	 */
	public final Stream<O> sampleFirst(long timespan, TimeUnit unit) {
		return sampleFirst(timespan, unit, getTimer());
	}


	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch.
	 *
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @param timer    the Timer to run on
	 * @return a new {@link Stream} whose values are the first value of each batch
	 */
	public final Stream<O> sampleFirst(long timespan, TimeUnit unit, Timer timer) {
		return sampleFirst(Integer.MAX_VALUE, timespan, unit, timer);
	}


	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch.
	 *
	 * @param maxSize  the max counted size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @return a new {@link Stream} whose values are the first value of each batch
	 */
	public final Stream<O> sampleFirst(int maxSize, long timespan, TimeUnit unit) {
		return sampleFirst(maxSize, timespan, unit, getTimer());
	}


	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch.
	 *
	 * @param maxSize  the max counted size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @param timer    the Timer to run on
	 * @return a new {@link Stream} whose values are the first value of each batch
	 */
	public final Stream<O> sampleFirst(final int maxSize, final long timespan, final TimeUnit unit, final Timer timer) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new SampleAction<O>(getDispatcher(), true, maxSize, timespan, unit, timer);
			}
		});
	}

	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code
	 * getCapacity()}
	 *
	 * @return a new {@link Stream} whose values are the last value of each batch
	 */
	public final Stream<O> sample() {
		return sample((int) Math.min(Integer.MAX_VALUE, getCapacity()));
	}


	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code
	 * getCapacity()}
	 *
	 * @param batchSize the batch size to use
	 * @return a new {@link Stream} whose values are the last value of each batch
	 */
	public final Stream<O> sample(final int batchSize) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new SampleAction<O>(getDispatcher(), batchSize);
			}
		});
	}


	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch.
	 *
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @return a new {@link Stream} whose values are the last value of each batch
	 */
	public final Stream<O> sample(long timespan, TimeUnit unit) {
		return sample(timespan, unit, getTimer());
	}


	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch.
	 *
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @param timer    the Timer to run on
	 * @return a new {@link Stream} whose values are the last value of each batch
	 */
	public final Stream<O> sample(long timespan, TimeUnit unit, Timer timer) {
		return sample(Integer.MAX_VALUE, timespan, unit, timer);
	}


	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch.
	 *
	 * @param maxSize  the max counted size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @return a new {@link Stream} whose values are the last value of each batch
	 */
	public final Stream<O> sample(int maxSize, long timespan, TimeUnit unit) {
		return sample(maxSize, timespan, unit, getEnvironment() == null ? Environment.timer() : getEnvironment().getTimer
				());
	}


	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch.
	 *
	 * @param maxSize  the max counted size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @param timer    the Timer to run on
	 * @return a new {@link Stream} whose values are the last value of each batch
	 */
	public final Stream<O> sample(final int maxSize, final long timespan, final TimeUnit unit, final Timer timer) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new SampleAction<O>(getDispatcher(), false, maxSize, timespan, unit, timer);
			}
		});
	}

	/**
	 * Create a new {@code Stream} that filters out consecutive equals values.
	 *
	 * @return a new {@link Stream} whose values are the last value of each batch
	 * @since 2.0
	 */
	public final Stream<O> distinctUntilChanged() {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new DistinctUntilChangedAction<O, O>(null);
			}
		});
	}

	/**
	 * Create a new {@code Stream} that filters out consecutive values having equal keys computed by function
	 *
	 * @param keySelector function to compute comparison key for each element
	 * @return a new {@link Stream} whose values are the last value of each batch
	 * @since 2.0
	 */
	public final <V> Stream<O> distinctUntilChanged(final Function<? super O, ? extends V> keySelector) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new DistinctUntilChangedAction<O, V>(keySelector);
			}
		});
	}

	/**
	 * Create a new {@code Stream} that filters in only unique values.
	 *
	 * @return a new {@link Stream} with unique values
	 */
	public final Stream<O> distinct() {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new DistinctAction<O, O>(null);
			}
		});
	}

	/**
	 * Create a new {@code Stream} that filters in only values having distinct keys computed by function
	 *
	 * @param keySelector function to compute comparison key for each element
	 * @return a new {@link Stream} with values having distinct keys
	 */
	public final <V> Stream<O> distinct(final Function<? super O, ? extends V> keySelector) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new DistinctAction<O, V>(keySelector);
			}
		});
	}

    /**
     * Create a new {@code Stream} that emits <code>true</code> when any value satisfies a predicate
     * and <code>false</code> otherwise
     *
     * @param predicate predicate tested upon values
     * @return a new {@link Stream} with <code>true</code> if any value satisfies a predicate
     *         and <code>false</code> otherwise
     * @since 2.0
     */
    public final Stream<Boolean> exists(final Predicate<? super O> predicate) {
        return lift(new Supplier<Action<O, Boolean>>() {
            @Override
            public Action<O, Boolean> get() {
                return new ExistsAction<O>(predicate);
            }
        });
    }

	/**
	 * Create a new {@code Stream} whose values will be each element E of any Iterable<E> flowing this Stream
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @return a new {@link Stream} whose values result from the iterable input
	 * @since 1.1, 2.0
	 */
	public final <V> Stream<V> split() {
		return split(Long.MAX_VALUE);
	}

	/**
	 * Create a new {@code Stream} whose values will be each element E of any Iterable<E> flowing this Stream
	 * <p>
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @param batchSize the batch size to use
	 * @return a new {@link Stream} whose values result from the iterable input
	 * @since 1.1, 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <V> Stream<V> split(final long batchSize) {
		final Stream<Iterable<? extends V>> iterableStream = (Stream<Iterable<? extends V>>) this;
		/*return iterableStream.flatMap(new Function<Iterable<V>, Publisher<? extends V>>() {
			@Override
			public Publisher<? extends V> apply(Iterable<V> vs) {
				return Streams.from(vs);
			}
		});*/
		return iterableStream.lift(new Supplier<Action<Iterable<? extends V>, V>>() {
			@Override
			public Action<Iterable<? extends V>, V> get() {
				return new SplitAction<V>().capacity(batchSize);
			}
		});
	}

	/**
	 * Collect incoming values into a {@link java.util.List} that will be pushed into the returned {@code Stream} every
	 * time {@link #getCapacity()} has been reached, or flush is triggered.
	 *
	 * @return a new {@link Stream} whose values are a {@link java.util.List} of all values in this batch
	 */
	public final Stream<List<O>> buffer() {
		return buffer((int) Math.min(Integer.MAX_VALUE, getCapacity()));
	}

	/**
	 * Collect incoming values into multiple {@link List} buckets that will be pushed into the returned {@code Stream}
	 * every time {@link #getCapacity()} has been reached.
	 *
	 * @param maxSize the collected size
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(final int maxSize) {
		return lift(new Supplier<Action<O, List<O>>>() {
			@Override
			public Action<O, List<O>> get() {
				return new BufferAction<O>(getDispatcher(), maxSize);
			}
		});
	}

	/**
	 * Collect incoming values into a {@link List} that will be moved into the returned {@code Stream} every time the
	 * passed boundary publisher emits an item.
	 * Complete will flush any remaining items.
	 *
	 * @param bucketOpening    the publisher to subscribe to on start for creating new buffer on next or complete
	 *                         signals.
	 * @param boundarySupplier the factory to provide a publisher to subscribe to when a buffer has been started
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(final Publisher<?> bucketOpening, final Supplier<? extends Publisher<?>>
			boundarySupplier) {

		return lift(new Supplier<Action<O, List<O>>>() {
			@Override
			public Action<O, List<O>> get() {
				return new BufferShiftWhenAction<O>(bucketOpening, boundarySupplier);
			}
		});
	}


	/**
	 * Collect incoming values into a {@link List} that will be moved into the returned {@code Stream} every time the
	 * passed boundary publisher emits an item.
	 * Complete will flush any remaining items.
	 *
	 * @param boundarySupplier the factory to provide a publisher to subscribe to on start for emiting and starting a
	 *                         new buffer
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(final Supplier<? extends Publisher<?>> boundarySupplier) {
		return lift(new Supplier<Action<O, List<O>>>() {
			@Override
			public Action<O, List<O>> get() {
				return new BufferWhenAction<O>(boundarySupplier);
			}
		});
	}


	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
	 * maxSize} has been reached by any of them. Complete signal will flush any remaining buckets.
	 *
	 * @param skip    the number of items to skip before creating a new bucket
	 * @param maxSize the collected size
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(final int maxSize, final int skip) {
		if (maxSize == skip) {
			return buffer(maxSize);
		}
		return lift(new Supplier<Action<O, List<O>>>() {
			@Override
			public Action<O, List<O>> get() {
				return new BufferShiftAction<O>(getDispatcher(), maxSize, skip);
			}
		});
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every
	 * timespan.
	 *
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(long timespan, TimeUnit unit) {
		return buffer(timespan, unit, getTimer());
	}


	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every
	 * timespan.
	 *
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @param timer    the Timer to run on
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(long timespan, TimeUnit unit, Timer timer) {
		return buffer(Integer.MAX_VALUE, timespan, unit, timer);
	}


	/**
	 * Collect incoming values into multiple {@link List} buckets created every {@code timeshift }that will be pushed
	 * into the returned {@code Stream} every
	 * timespan. Complete signal will flush any remaining buckets.
	 *
	 * @param timespan  the period in unit to use to release buffered lists
	 * @param timeshift the period in unit to use to create a new bucket
	 * @param unit      the time unit
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(final long timespan, final long timeshift, final TimeUnit unit) {
		return buffer(timespan, timeshift, unit, getTimer());
	}

	/**
	 * Collect incoming values into multiple {@link List} buckets created every {@code timeshift }that will be pushed
	 * into the returned {@code Stream} every
	 * timespan. Complete signal will flush any remaining buckets.
	 *
	 * @param timespan  the period in unit to use to release buffered lists
	 * @param timeshift the period in unit to use to create a new bucket
	 * @param unit      the time unit
	 * @param timer     the Timer to run on
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(final long timespan, final long timeshift, final TimeUnit unit, final Timer
			timer) {
		if (timespan == timeshift) {
			return buffer(timespan, unit, timer);
		}
		return lift(new Supplier<Action<O, List<O>>>() {
			@Override
			public Action<O, List<O>> get() {
				return new BufferShiftAction<O>(getDispatcher(), Integer.MAX_VALUE, Integer.MAX_VALUE, timeshift, timespan,
						unit,
						timer);
			}
		});
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every
	 * timespan OR maxSize items.
	 *
	 * @param maxSize  the max collected size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(int maxSize, long timespan, TimeUnit unit) {
		return buffer(maxSize, timespan, unit, getTimer());
	}


	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every
	 * timespan OR maxSize items
	 *
	 * @param maxSize  the max collected size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @param timer    the Timer to run on
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(final int maxSize, final long timespan, final TimeUnit unit, final Timer timer) {
		return lift(new Supplier<Action<O, List<O>>>() {
			@Override
			public Action<O, List<O>> get() {
				return new BufferAction<O>(getDispatcher(), maxSize, timespan, unit, timer);
			}
		});
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the
	 * returned fresh {@link Stream}. Possible flush triggers are: {@link this#getCapacity()},
	 * complete signal or request signal.
	 * PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 *
	 * @return a new {@link Stream} whose values re-ordered using a PriorityQueue.
	 * @since 2.0
	 */
	public final Stream<O> sort() {
		return sort(null);
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the
	 * returned fresh {@link Stream}. Possible flush triggers are: {@link this#getCapacity()},
	 * complete signal or request signal.
	 * PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 *
	 * @param maxCapacity a fixed maximum number or elements to re-order at once.
	 * @return a new {@link Stream} whose values re-ordered using a PriorityQueue.
	 * @since 2.0
	 */
	public final Stream<O> sort(int maxCapacity) {
		return sort(maxCapacity, null);
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the
	 * returned fresh {@link Stream}. Possible flush triggers are: {@link this#getCapacity()},
	 * complete signal or request signal.
	 * PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 *
	 * @param comparator A {@link Comparator<O>} to evaluate incoming data
	 * @return a new {@link Stream} whose values re-ordered using a PriorityQueue.
	 * @since 2.0
	 */
	public final Stream<O> sort(Comparator<? super O> comparator) {
		return sort((int) Math.min(Integer.MAX_VALUE, getCapacity()), comparator);
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the
	 * returned fresh {@link Stream}. Possible flush triggers are: {@link this#getCapacity()},
	 * complete signal or request signal.
	 * PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 *
	 * @param maxCapacity a fixed maximum number or elements to re-order at once.
	 * @param comparator  A {@link Comparator<O>} to evaluate incoming data
	 * @return a new {@link Stream} whose values re-ordered using a PriorityQueue.
	 * @since 2.0
	 */
	public final Stream<O> sort(final int maxCapacity, final Comparator<? super O> comparator) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new SortAction<O>(getDispatcher(), maxCapacity, comparator);
			}
		});
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined {@link this#getCapacity()}
	 * times. The nested streams will be pushed into the returned {@code Stream}.
	 *
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final Stream<Stream<O>> window() {
		return window((int) Math.min(Integer.MAX_VALUE, getCapacity()));
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined {@param backlog} times.
	 * The nested streams will be pushed into the returned {@code Stream}.
	 *
	 * @param backlog the time period when each window close and flush the attached consumer
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final Stream<Stream<O>> window(final int backlog) {
		return lift(new Supplier<Action<O, Stream<O>>>() {
			@Override
			public Action<O, Stream<O>> get() {
				return new WindowAction<O>(getEnvironment(), getDispatcher(), backlog);
			}
		});
	}

	/**
	 * Re-route incoming values into bucket streams that will be pushed into the returned {@code Stream} every {@code
	 * skip} and complete every time {@code
	 * maxSize} has been reached by any of them. Complete signal will flush any remaining buckets.
	 *
	 * @param skip    the number of items to skip before creating a new bucket
	 * @param maxSize the collected size
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 */
	public final Stream<Stream<O>> window(final int maxSize, final int skip) {
		if (maxSize == skip) {
			return window(maxSize);
		}
		return lift(new Supplier<Action<O, Stream<O>>>() {
			@Override
			public Action<O, Stream<O>> get() {
				return new WindowShiftAction<O>(getEnvironment(), getDispatcher(), maxSize, skip);
			}
		});
	}

	/**
	 * Re-route incoming values into bucket streams that will be pushed into the returned {@code Stream} every  and
	 * complete every time {@code boundarySupplier} stream emits an item.
	 *
	 * @param boundarySupplier the factory to create the stream to listen to for separating each window
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 */
	public final Stream<Stream<O>> window(final Supplier<? extends Publisher<?>> boundarySupplier) {
		return lift(new Supplier<Action<O, Stream<O>>>() {
			@Override
			public Action<O, Stream<O>> get() {
				return new WindowWhenAction<O>(getEnvironment(), getDispatcher(), boundarySupplier);
			}
		});
	}

	/**
	 * Re-route incoming values into bucket streams that will be pushed into the returned {@code Stream} every and
	 * complete every time {@code boundarySupplier} stream emits an item. Window starts forwarding when the
	 * bucketOpening stream emits an item, then subscribe to the boundary supplied to complete.
	 *
	 * @param bucketOpening    the publisher to listen for signals to create a new window
	 * @param boundarySupplier the factory to create the stream to listen to for closing an open window
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 */
	public final Stream<Stream<O>> window(final Publisher<?> bucketOpening, final Supplier<? extends Publisher<?>>
			boundarySupplier) {
		return lift(new Supplier<Action<O, Stream<O>>>() {
			@Override
			public Action<O, Stream<O>> get() {
				return new WindowShiftWhenAction<O>(getEnvironment(), getDispatcher(), bucketOpening, boundarySupplier);
			}
		});
	}


	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined timespan.
	 * The nested streams will be pushed into the returned {@code Stream}.
	 *
	 * @param timespan the period in unit to use to release a new window as a Stream
	 * @param unit     the time unit
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final Stream<Stream<O>> window(long timespan, TimeUnit unit) {
		return window(timespan, unit, getTimer());
	}


	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined timespan.
	 * The nested streams will be pushed into the returned {@code Stream}.
	 *
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @param timer    the Timer to run on
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final Stream<Stream<O>> window(long timespan, TimeUnit unit, Timer timer) {
		return window(Integer.MAX_VALUE, timespan, unit, timer);
	}


	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined timespan OR maxSize items.
	 * The nested streams will be pushed into the returned {@code Stream}.
	 *
	 * @param maxSize  the max collected size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final Stream<Stream<O>> window(int maxSize, long timespan, TimeUnit unit) {
		return window(maxSize, timespan, unit, getEnvironment() == null ? Environment.timer() : getEnvironment().getTimer
				());
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined timespan OR maxSize items.
	 * The nested streams will be pushed into the returned {@code Stream}.
	 *
	 * @param maxSize  the max collected size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit     the time unit
	 * @param timer    the Timer to run on
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final Stream<Stream<O>> window(final int maxSize, final long timespan, final TimeUnit unit, final Timer
			timer) {
		return lift(new Supplier<Action<O, Stream<O>>>() {
			@Override
			public Action<O, Stream<O>> get() {
				return new WindowAction<O>(getEnvironment(), getDispatcher(), maxSize, timespan, unit, timer);
			}
		});
	}

	/**
	 * Re-route incoming values into bucket streams that will be pushed into the returned {@code Stream} every {@code
	 * timeshift} period. These streams will complete every {@code
	 * timespan} period has cycled. Complete signal will flush any remaining buckets.
	 *
	 * @param timespan  the period in unit to use to complete a window
	 * @param timeshift the period in unit to use to create a new window
	 * @param unit      the time unit
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 */
	public final Stream<Stream<O>> window(final long timespan, final long timeshift, final TimeUnit unit) {
		return window(timespan, timeshift, unit, getTimer());
	}


	/**
	 * Re-route incoming values into bucket streams that will be pushed into the returned {@code Stream} every {@code
	 * timeshift} period. These streams will complete every {@code
	 * timespan} period has cycled. Complete signal will flush any remaining buckets.
	 *
	 * @param timespan  the period in unit to use to complete a window
	 * @param timeshift the period in unit to use to create a new window
	 * @param unit      the time unit
	 * @param timer     the Timer to run on
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 */
	public final Stream<Stream<O>> window(final long timespan, final long timeshift, final TimeUnit unit, final Timer
			timer) {
		if (timeshift == timespan) {
			return window(timespan, unit, timer);
		}
		return lift(new Supplier<Action<O, Stream<O>>>() {
			@Override
			public Action<O, Stream<O>> get() {
				return new WindowShiftAction<O>(
						getEnvironment(), getDispatcher(), Integer.MAX_VALUE, Integer.MAX_VALUE, timespan, timeshift, unit, timer);
			}
		});
	}


	/**
	 * Re-route incoming values into a dynamically created {@link Stream} for each unique key evaluated by the
	 * {param keyMapper}.
	 *
	 * @param keyMapper the key mapping function that evaluates an incoming data and returns a key.
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final <K> Stream<GroupedStream<K, O>> groupBy(final Function<? super O, ? extends K> keyMapper) {
		return lift(new Supplier<Action<O, GroupedStream<K, O>>>() {
			@Override
			public Action<O, GroupedStream<K, O>> get() {
				return new GroupByAction<>(getEnvironment(), keyMapper, getDispatcher());
			}
		});
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} for each unique key evaluated by the
	 * {param keyMapper}. The hashcode of the incoming data will be used for partitioning over {@link
	 * Environment#PROCESSORS} buckets.
	 * That means that at any point of time at most {@link Environment#PROCESSORS} number of streams will be created and
	 * used accordingly
	 * to the current hashcode % n result.
	 *
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values routed to this partition
	 * @since 2.0
	 */
	public final Stream<GroupedStream<Integer, O>> partition() {
		return partition(Environment.PROCESSORS);
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} for each unique key evaluated by the
	 * {param keyMapper}. The hashcode of the incoming data will be used for partitioning over the buckets number passed.
	 * That means that at any point of time at most {@code buckets} number of streams will be created and used
	 * accordingly to the positive modulo of the current hashcode with respect to the number of buckets specified.
	 *
	 * @param buckets the maximum number of buckets to partition the values across
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values routed to this partition
	 * @since 2.0
	 */
	public final Stream<GroupedStream<Integer, O>> partition(final int buckets) {
		return groupBy(new Function<O, Integer>() {
			@Override
			public Integer apply(O o) {
				int bucket = o.hashCode() % buckets;
				return bucket < 0 ? bucket + buckets : bucket;
			}
		});
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code T}.
	 * This is a simple functional way for accumulating values.
	 * The arguments are the N-1 and N next signal in this order.
	 *
	 * @param fn the reduce function
	 * @return a new {@link Stream} whose values contain only the reduced objects
	 */
	public final Stream<O> reduce(@Nonnull final BiFunction<O, O, O> fn) {
		return scan(fn).last();
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}.
	 * The arguments are the N-1 and N next signal in this order.
	 *
	 * @param fn      the reduce function
	 * @param initial the initial argument to pass to the reduce function
	 * @param <A>     the type of the reduced object
	 * @return a new {@link Stream} whose values contain only the reduced objects
	 */
	public final <A> Stream<A> reduce(final A initial, @Nonnull BiFunction<A, ? super O, A> fn) {

		return scan(initial, fn).last();
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}.
	 * The arguments are the N-1 and N next signal in this order.
	 *
	 * @param fn the reduce function
	 * @return a new {@link Stream} whose values contain only the reduced objects
	 * @since 1.1, 2.0
	 */
	public final Stream<O> scan(@Nonnull final BiFunction<O, O, O> fn) {
		return scan(null, fn);
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}. The given initial object will be
	 * passed to the function's {@link Tuple2} argument. Behave like Reduce but triggers downstream Stream for every
	 * transformation.
	 *
	 * @param initial the initial argument to pass to the reduce function
	 * @param fn      the scan function
	 * @param <A>     the type of the reduced object
	 * @return a new {@link Stream} whose values contain only the reduced objects
	 * @since 1.1, 2.0
	 */
	public final <A> Stream<A> scan(final A initial, final @Nonnull BiFunction<A, ? super O, A> fn) {
		return lift(new Supplier<Action<O, A>>() {
			@Override
			public Action<O, A> get() {
				return new ScanAction<O, A>(initial, fn);
			}
		});
	}

	/**
	 * Count accepted events for each batch and pass each accumulated long to the {@param stream}.
	 */
	public final Stream<Long> count() {
		return count(getCapacity());
	}

	/**
	 * Count accepted events for each batch {@param i} and pass each accumulated long to the {@param stream}.
	 *
	 * @return a new {@link Stream}
	 */
	public final Stream<Long> count(final long i) {
		return lift(new Supplier<Action<O, Long>>() {
			@Override
			public Action<O, Long> get() {
				return new CountAction<O>(i);
			}
		});
	}

	/**
	 * Request once the parent stream every {@param period} milliseconds. Timeout is run on the environment root timer.
	 *
	 * @param period the period in milliseconds between two notifications on this stream
	 * @return a new {@link Stream}
	 * @since 2.0
	 */
	public final Stream<O> throttle(long period) {
		Timer timer = getTimer();
		Assert.state(timer != null, "Cannot use default timer as no environment has been provided to this " +
				"Stream");
		return throttle(period, timer);
	}


	/**
	 * Request once the parent stream every {@param period} milliseconds after an initial {@param delay}.
	 * Timeout is run on the given {@param timer}.
	 *
	 * @param period the timeout in milliseconds between two notifications on this stream
	 * @param timer  the reactor timer to run the timeout on
	 * @return a new {@link Stream}
	 * @since 2.0
	 */
	public final Stream<O> throttle(final long period, final Timer timer) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new ThrottleRequestAction<O>(
						getDispatcher(),
						timer,
						period
				);
			}
		});
	}

	/**
	 * Request the parent stream every time the passed throttleStream signals a Long request volume. Complete and Error
	 * signals will be propagated.
	 *
	 * @param throttleStream a function that takes a broadcasted stream of request signals and must return a stream of
	 *                       valid request signal (long).
	 * @return a new {@link Stream}
	 * @since 2.0
	 */
	public final Stream<O> requestWhen(final Function<? super Stream<? extends Long>, ? extends Publisher<? extends
			Long>> throttleStream) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new ThrottleRequestWhenAction<O>(
						getDispatcher(),
						throttleStream
				);
			}
		});
	}

	/**
	 * Signal an error if no data has been emitted for {@param
	 * timeout} milliseconds. Timeout is run on the environment root timer.
	 * <p>
	 * A Timeout Exception will be signaled if no data or complete signal have been sent within the given period.
	 *
	 * @param timeout the timeout in milliseconds between two notifications on this composable
	 * @return a new {@link Stream}
	 * @since 1.1, 2.0
	 */
	public final Stream<O> timeout(long timeout) {
		return timeout(timeout, null);
	}

	/**
	 * Signal an error if no data has been emitted for {@param
	 * timeout} milliseconds. Timeout is run on the environment root timer.
	 * <p>
	 * A Timeout Exception will be signaled if no data or complete signal have been sent within the given period.
	 *
	 * @param timeout the timeout in unit between two notifications on this composable
	 * @param unit    the time unit
	 * @return a new {@link Stream}
	 * @since 1.1, 2.0
	 */
	public final Stream<O> timeout(long timeout, TimeUnit unit) {
		return timeout(timeout, unit, null);
	}

	/**
	 * Switch to the fallback Publisher if no data has been emitted for {@param
	 * timeout} milliseconds. Timeout is run on the environment root timer.
	 * <p>
	 * The current subscription will be cancelled and the fallback publisher subscribed.
	 * <p>
	 * A Timeout Exception will be signaled if no data or complete signal have been sent within the given period.
	 *
	 * @param timeout  the timeout in unit between two notifications on this composable
	 * @param unit     the time unit
	 * @param fallback the fallback {@link Publisher} to subscribe to once the timeout has occured
	 * @return a new {@link Stream}
	 * @since 2.0
	 */
	public final Stream<O> timeout(long timeout, TimeUnit unit, Publisher<? extends O> fallback) {
		Timer timer = getTimer();
		Assert.state(timer != null, "Cannot use default timer as no environment has been provided to this " +
				"Stream");
		return timeout(timeout, unit, fallback, timer);
	}

	/**
	 * Signal an error if no data has been emitted for {@param
	 * timeout} milliseconds. Timeout is run on the environment root timer.
	 * <p>
	 * A Timeout Exception will be signaled if no data or complete signal have been sent within the given period.
	 *
	 * @param timeout the timeout in milliseconds between two notifications on this composable
	 * @param unit    the time unit
	 * @param timer   the reactor timer to run the timeout on
	 * @return a new {@link Stream}
	 * @since 1.1, 2.0
	 */
	public final Stream<O> timeout(final long timeout, final TimeUnit unit, final Publisher<? extends O> fallback, final
	Timer timer) {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new TimeoutAction<O>(
						getDispatcher(),
						fallback,
						timer,
						unit != null ? TimeUnit.MILLISECONDS.convert(timeout, unit) : timeout
				);
			}
		});
	}

	/**
	 * Combine the most ancient upstream action to act as the {@link org.reactivestreams.Subscriber} input component and
	 * the current stream to act as the {@link org.reactivestreams.Publisher}.
	 * <p>
	 * Useful to share and ship a full stream whilst hiding the staging actions in the middle.
	 * <p>
	 * Default behavior, e.g. a single stream, will raise an {@link java.lang.IllegalStateException} as there would not
	 * be any Subscriber (Input) side to combine. {@link reactor.rx.action.Action#combine()} is the usual reference
	 * implementation used.
	 *
	 * @param <E> the type of the most ancien action input.
	 * @return new Combined Action
	 */
	public <E> CompositeAction<E, O> combine() {
		throw new IllegalStateException("Cannot combine a single Stream");
	}

	/**
	 * Return the promise of the next triggered signal.
	 * A promise is a container that will capture only once the first arriving error|next|complete signal
	 * to this {@link Stream}. It is useful to coordinate on single data streams or await for any signal.
	 *
	 * @return a new {@link Promise}
	 * @since 2.0
	 */
	public final Promise<O> next() {
		Promise<O> d = new Promise<O>(
				getDispatcher(),
				getEnvironment()
		);
		subscribe(d);
		return d;
	}

	/**
	 * Fetch all values in a List to the returned Promise
	 *
	 * @return the promise of all data from this Stream
	 * @since 2.0
	 */
	public final Promise<List<O>> toList() {
		return toList(-1);
	}

	/**
	 * Return the promise of N signals collected into an array list.
	 *
	 * @param maximum list size and therefore events signal to listen for
	 * @return the promise of all data from this Stream
	 * @since 2.0
	 */
	public final Promise<List<O>> toList(long maximum) {
		if (maximum > 0)
			return take(maximum).buffer().next();
		else {
			return buffer().next();
		}
	}

	/**
	 * Assign an Environment to be provided to this Stream Subscribers
	 *
	 * @param environment the environment
	 * @return a
	 */
	public Stream<O> env(final Environment environment) {
		return new Stream<O>() {
			@Override
			public void subscribe(Subscriber<? super O> s) {
				Stream.this.subscribe(s);
			}

			@Override
			public long getCapacity() {
				return Stream.this.getCapacity();
			}

			@Override
			public Dispatcher getDispatcher() {
				return Stream.this.getDispatcher();
			}

			@Override
			public Environment getEnvironment() {
				return environment;
			}
		};
	}

	/**
	 * Blocking call to pass values from this stream to the queue that can be polled from a consumer.
	 *
	 * @return the buffered queue
	 * @since 2.0
	 */
	public final CompletableBlockingQueue<O> toBlockingQueue() {
		return toBlockingQueue(-1);
	}


	/**
	 * Blocking call to eagerly fetch values from this stream
	 *
	 * @param maximum queue getCapacity(), a full queue might block the stream producer.
	 * @return the buffered queue
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final CompletableBlockingQueue<O> toBlockingQueue(int maximum) {
		final CompletableBlockingQueue<O> blockingQueue;
		Stream<O> tail = this;
		if (maximum > 0) {
			blockingQueue = new CompletableBlockingQueue<O>(maximum);
			tail = take(maximum);
		} else {
			blockingQueue = new CompletableBlockingQueue<O>(1);
		}

		Consumer terminalConsumer = new Consumer<Object>() {
			@Override
			public void accept(Object o) {
				blockingQueue.complete();
			}
		};

		ConsumerAction<O> callbackAction = new ConsumerAction<O>(getDispatcher(), new Consumer<O>() {
			@Override
			public void accept(O o) {
				try {
					blockingQueue.put(o);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}
		}, terminalConsumer, terminalConsumer);

		tail.subscribe(callbackAction);

		return blockingQueue;
	}

	/**
	 * Prevent a {@link Stream} to be cancelled. Cancel propagation occurs when last subscriber is cancelled.
	 *
	 * @return a new {@literal Stream} that is never cancelled.
	 */
	public Stream<O> keepAlive() {
		return lift(new Supplier<Action<O, O>>() {
			@Override
			public Action<O, O> get() {
				return new Action<O, O>() {
					@Override
					protected void doNext(O ev) {
						broadcastNext(ev);
					}

					@Override
					protected void doShutdown() {
						//IGNORE
					}
				};
			}
		});
	}

	/**
	 * Subscribe the {@link reactor.rx.action.CompositeAction#input()} to this Stream. Combining action
	 * through {@link
	 * reactor.rx.action.Action#combine()} allows for easy distribution of a full flow.
	 *
	 * @param subscriber the combined actions to subscribe
	 * @since 2.0
	 */
	public final <A> void subscribe(final CompositeAction<O, A> subscriber) {
		subscribe(subscriber.input());
	}

	@Override
	public long getCapacity() {
		return Long.MAX_VALUE;
	}

	@Override
	public boolean isReactivePull(Dispatcher dispatcher, long producerCapacity) {
		return (getCapacity() < producerCapacity)
				&& getDispatcher().getClass() != TailRecurseDispatcher.class
				&& dispatcher.getClass() != TailRecurseDispatcher.class;
	}


	/**
	 * Get the current timer available if any or try returning the shared Environment one (which may cause an exception
	 * if no Environment has been globally initialized)
	 *
	 * @return any available timer
	 */
	public Timer getTimer() {
		return getEnvironment() == null ? Environment.timer() : getEnvironment().getTimer();
	}

	/**
	 * Get the current action child subscription
	 *
	 * @return current child {@link reactor.rx.subscription.PushSubscription}
	 */
	public PushSubscription<O> downstreamSubscription() {
		return null;
	}

	/**
	 * Try cleaning a given subscription from the stream references. Unicast implementation such as IterableStream
	 * (Streams.from(1,2,3)) or SupplierStream (Streams.generate(-> 1)) won't need to perform any job and as such will
	 * return @{code false} upon this call.
	 * Alternatively, Action and HotStream (Streams.from()) will clean any reference to that subscription from their
	 * internal registry and might return {@code true} if successful.
	 *
	 * @return current child {@link reactor.rx.subscription.PushSubscription}
	 */
	public boolean cancelSubscription(PushSubscription<O> oPushSubscription) {
		return false;
	}

	/**
	 * Get the assigned {@link reactor.Environment}.
	 *
	 * @return current {@link Environment}
	 */
	public Environment getEnvironment() {
		return null;
	}

	/**
	 * Get the dispatcher used to execute signals on this Stream instance.
	 *
	 * @return assigned dispatcher
	 */
	public Dispatcher getDispatcher() {
		return SynchronousDispatcher.INSTANCE;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName();
	}

}
