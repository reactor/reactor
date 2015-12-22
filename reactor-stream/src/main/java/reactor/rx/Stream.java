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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import javax.annotation.Nonnull;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Processors;
import reactor.Publishers;
import reactor.Timers;
import reactor.core.processor.BaseProcessor;
import reactor.core.processor.ProcessorGroup;
import reactor.core.publisher.operator.IgnoreOnNextOperator;
import reactor.core.publisher.operator.LogOperator;
import reactor.core.publisher.operator.MapOperator;
import reactor.core.publisher.operator.OnErrorResumeOperator;
import reactor.core.publisher.operator.ZipOperator;
import reactor.core.support.Assert;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.core.timer.Timer;
import reactor.fn.BiConsumer;
import reactor.fn.BiFunction;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.fn.Supplier;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.fn.tuple.TupleN;
import reactor.rx.action.Control;
import reactor.rx.action.DemandControl;
import reactor.rx.action.Signal;
import reactor.rx.action.StreamProcessor;
import reactor.rx.action.BatchOperator;
import reactor.rx.action.BufferOperator;
import reactor.rx.action.BufferShiftOperator;
import reactor.rx.action.BufferShiftWhenOperator;
import reactor.rx.action.BufferWhenOperator;
import reactor.rx.action.LastOperator;
import reactor.rx.action.SampleOperator;
import reactor.rx.action.SortOperator;
import reactor.rx.action.WindowOperator;
import reactor.rx.action.WindowShiftOperator;
import reactor.rx.action.WindowShiftWhenOperator;
import reactor.rx.action.WindowWhenOperator;
import reactor.rx.action.SwitchOperator;
import reactor.rx.action.ZipWithIterableOperator;
import reactor.rx.action.ExistsOperator;
import reactor.rx.action.DropOperator;
import reactor.rx.action.RepeatOperator;
import reactor.rx.action.RepeatWhenOperator;
import reactor.rx.action.ThrottleRequestOperator;
import reactor.rx.action.ThrottleRequestWhenOperator;
import reactor.rx.action.ErrorOperator;
import reactor.rx.action.ErrorWithValueOperator;
import reactor.rx.action.RetryOperator;
import reactor.rx.action.RetryWhenOperator;
import reactor.rx.action.TimeoutOperator;
import reactor.rx.action.DistinctOperator;
import reactor.rx.action.DistinctUntilChangedOperator;
import reactor.rx.action.ElementAtOperator;
import reactor.rx.action.FilterOperator;
import reactor.rx.action.SkipOperator;
import reactor.rx.action.SkipUntilTimeoutOperator;
import reactor.rx.action.TakeOperator;
import reactor.rx.action.TakeUntilTimeoutOperator;
import reactor.rx.action.TakeWhileOperator;
import reactor.rx.action.CountOperator;
import reactor.rx.action.ElapsedOperator;
import reactor.rx.action.CallbackOperator;
import reactor.rx.action.FinallyOperator;
import reactor.rx.action.StreamStateCallbackOperator;
import reactor.rx.action.TapAndControls;
import reactor.rx.action.Tap;
import reactor.rx.action.DefaultIfEmptyOperator;
import reactor.rx.action.DematerializeOperator;
import reactor.rx.action.GroupByOperator;
import reactor.rx.action.MaterializeOperator;
import reactor.rx.action.ScanOperator;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.stream.GroupedStream;
import reactor.rx.subscriber.AdaptiveSubscriber;
import reactor.rx.subscriber.BoundedSubscriber;
import reactor.rx.subscriber.InterruptableSubscriber;
import reactor.rx.subscriber.ManualSubscriber;

/**
 * Base class for components designed to provide a succinct API for working with future values. Provides base
 * functionality and an internal contract for subclasses. <p> A Stream can be implemented to perform specific actions on
 * callbacks (onNext,onComplete,onError,onSubscribe). Stream can eventually produce result data {@code <O>} and will
 * offer error cascading over to its subscribers. <p> <p> Typically, new {@code Stream} aren't created directly. To
 * create a {@code Stream}, use {@link Streams} static API.
 * @param <O> The type of the output values
 * @author Stephane Maldini
 * @author Jon Brisbin
 * @since 1.1, 2.0
 */
public abstract class Stream<O> implements Publisher<O>, ReactiveState.Bounded {

	protected Stream() {
	}

	/**
	 * Cast the current Stream flowing data type into a target class type.
	 * @param <E> the {@link Stream} output type
	 * @return the current {link Stream} instance casted
	 * @since 2.0
	 */
	@SuppressWarnings({"unchecked", "unused"})
	public final <E> Stream<E> cast(@Nonnull final Class<E> stream) {
		return (Stream<E>) this;
	}

	/**
	 * @see {@link Publishers#lift(Publisher, Function)}
	 * @since 2.1
	 */
	public <V> Stream<V> lift(@Nonnull final Function<Subscriber<? super V>, Subscriber<? super O>> operator) {
		return Streams.lift(this, operator);
	}

	/**
	 * Defer the subscription of a {@link Processor} to the actual pipeline. Terminal operations such as {@link
	 * #consume(reactor.fn.Consumer)} will start the subscription chain. It will listen for current Stream signals and
	 * will be eventually producing signals as well (subscribe,error, complete,next). <p> The action is returned for
	 * functional-style chaining.
	 * @param <V> the {@link reactor.rx.Stream} output type
	 * @param processorSupplier the function to map a provided dispatcher to a fresh Action to subscribe.
	 * @return the passed action
	 * @see {@link org.reactivestreams.Publisher#subscribe(org.reactivestreams.Subscriber)}
	 * @since 2.0
	 */
	public <V> Stream<V> liftProcessor(@Nonnull final Supplier<? extends Processor<O, V>> processorSupplier) {
		return lift(new Publishers.Operator<O, V>() {
			@Override
			public Subscriber<? super O> apply(Subscriber<? super V> subscriber) {
				Processor<O, V> processor = processorSupplier.get();
				processor.subscribe(subscriber);
				return processor;
			}
		});
	}

	/**
	 * Assign an error handler to exceptions of the given type. Will not stop error propagation, use when(class,
	 * publisher), retry, ignoreError or recover to actively deal with the error
	 * @param exceptionType the type of exceptions to handle
	 * @param onError the error handler for each error
	 * @param <E> type of the error to handle
	 * @return {@literal new Stream}
	 */
	@SuppressWarnings("unchecked")
	public final <E extends Throwable> Stream<O> when(@Nonnull final Class<E> exceptionType,
			@Nonnull final Consumer<E> onError) {
		return lift(new ErrorOperator<O, E>(exceptionType, onError));
	}

	/**
	 * Assign an error handler that will pass eventual associated values and exceptions of the given type. Will not stop
	 * error propagation, use when(class, publisher), retry, ignoreError or recover to actively deal with the error.
	 * @param exceptionType the type of exceptions to handle
	 * @param onError the error handler for each error
	 * @param <E> type of the error to handle
	 * @return {@literal new Stream}
	 */
	@SuppressWarnings("unchecked")
	public final <E extends Throwable> Stream<O> observeError(@Nonnull final Class<E> exceptionType,
			@Nonnull final BiConsumer<Object, ? super E> onError) {
		return lift(new ErrorWithValueOperator<O, E>(exceptionType, onError, null));
	}

	/**
	 * Produce a default value if any error occurs.
	 * @param fallback the error handler for each error
	 * @return {@literal new Stream}
	 */
	public final Stream<O> onErrorReturn(@Nonnull final O fallback) {
		return onErrorResumeNext(Streams.just(fallback));
	}

	/**
	 * Subscribe to a fallback publisher when any error occurs.
	 * @param fallback the error handler for each error
	 * @return {@literal new Stream}
	 */
	public final Stream<O> onErrorResumeNext(@Nonnull final Publisher<? extends O> fallback) {
		return lift(new OnErrorResumeOperator<O>(fallback));
	}

	/**
	 * Subscribe to a returned fallback publisher when any error occurs.
	 * @param fallback the error handler for each error
	 * @return {@literal new Stream}
	 */
	public final Stream<O> onErrorResumeNext(@Nonnull final Function<Throwable, ? extends Publisher<? extends O>>
			fallback) {
		return lift(new OnErrorResumeOperator<>(fallback));
	}

	/**
	 * @see reactor.Publishers#after(Publisher)
	 * @return {@literal new Stream}
	 */
	@SuppressWarnings("unchecked")
	public final Stream<Void> after() {
		return lift(IgnoreOnNextOperator.INSTANCE);
	}

	/**
	 * @see reactor.Publishers#ignoreElements(Publisher)
	 * @return {@literal new Stream}
	 */
	@SuppressWarnings("unchecked")
	public final Stream<O> ignoreElements() {
		return lift(IgnoreOnNextOperator.INSTANCE);
	}

	/**
	 * Transform the incoming onSubscribe, onNext, onError and onComplete signals into {@link reactor.rx.action
	 * .Signal}. Since the error is materialized as a {@code Signal}, the propagation will be stopped. Complete signal
	 * will first emit a {@code Signal.complete()} and then effectively complete the stream.
	 * @return {@literal new Stream}
	 */
	@SuppressWarnings("unchecked")
	public final Stream<Signal<O>> materialize() {
		return lift(MaterializeOperator.INSTANCE);
	}

	/**
	 * Transform the incoming onSubscribe, onNext, onError and onComplete signals into {@link reactor.rx.action
	 * .Signal}. Since the error is materialized as a {@code Signal}, the propagation will be stopped. Complete signal
	 * will first emit a {@code Signal.complete()} and then effectively complete the stream.
	 * @return {@literal new Stream}
	 */
	@SuppressWarnings("unchecked")
	public final <X> Stream<X> dematerialize() {
		Stream<Signal<X>> thiz = (Stream<Signal<X>>) this;
		return thiz.lift(DematerializeOperator.INSTANCE);
	}

	/**
	 * Subscribe a new {@link Broadcaster} and return it for future subscribers interactions. Effectively it turns any
	 * stream into an Hot Stream where subscribers will only values from the time T when they subscribe to the returned
	 * stream. Complete and Error signals are however retained. <p>
	 * @return a new {@literal stream} whose values are broadcasted to all subscribers
	 */
	public final Stream<O> broadcast() {
		Broadcaster<O> broadcaster = Broadcaster.create(getTimer());
		return broadcastTo(broadcaster);
	}

	/**
	 * Subscribe the passed subscriber, only creating once necessary upstream Subscriptions and returning itself. Mostly
	 * used by other broadcast actions which transform any Stream into a publish-subscribe Stream (every subscribers see
	 * all values). <p>
	 * @param subscriber the subscriber to subscribe to this stream and return
	 * @param <E> the hydrated generic type for the passed argument, allowing for method chaining
	 * @return {@param subscriber}
	 */
	public final <E extends Subscriber<? super O>> E broadcastTo(E subscriber) {
		subscribe(subscriber);
		return subscriber;
	}

	/**
	 * Create a {@link Tap} that maintains a reference to the last value seen by this {@code Stream}. The {@link Tap} is
	 * continually updated when new values pass through the {@code Stream}.
	 * @return the new {@link Tap}
	 * @see Consumer
	 */
	public final TapAndControls<O> tap() {
		final Tap<O> tap = Tap.create();
		return new TapAndControls<>(tap, consume(tap));
	}

	/**
	 * FIXME Doc
	 */
	@SuppressWarnings("unchecked")
	public final <E> Stream<E> process(final Processor<O, E> processor) {
		subscribe(processor);
		if (Stream.class.isAssignableFrom(processor.getClass())) {
			return (Stream<E>) processor;
		}

		return new Lift<O, E>(this){
			@Override
			public void subscribe(Subscriber s) {
				processor.subscribe(s);
			}

			@Override
			public String getName() {
				return "process";
			}
		};
	}

	/**
	 *
	 */
	public final Stream<O> dispatchOn(final ProcessorGroup processorProvider) {
		return new DispatchOnLift<>(this, processorProvider);
	}

	/**
	 *
	 */
	public final Stream<O> publishOn(final ProcessorGroup processorProvider) {
		return new PublishOnLift<>(this, processorProvider);
	}

	/**
	 * Defer a Controls operations ready to be requested.
	 * @return the consuming action
	 */
	public DemandControl consumeLater() {
		return consumeLater(null);
	}


	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will consume any values accepted by this {@code Stream}. As
	 * such this a terminal action to be placed on a stream flow. It will also eagerly prefetch upstream publisher. <p>
	 * For a passive version that observe and forward incoming data see {@link #observe(reactor.fn.Consumer)}
	 * @param consumer the consumer to invoke on each value
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public final DemandControl consumeLater(final Consumer<? super O> consumer) {
		return consumeLater(consumer, null, null);
	}

	/**
	 * Attach 2 {@link Consumer} to this {@code Stream} that will consume any values signaled by this {@code Stream}. As
	 * such this a terminal action to be placed on a stream flow. Any Error signal will be consumed by the error
	 * consumer. It will also eagerly prefetch upstream publisher. <p>
	 * @param consumer the consumer to invoke on each next signal
	 * @param errorConsumer the consumer to invoke on each error signal
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public final DemandControl consumeLater(final Consumer<? super O> consumer, Consumer<? super Throwable> errorConsumer) {
		return consumeLater(consumer, errorConsumer, null);
	}

	/**
	 * Attach 3 {@link Consumer} to this {@code Stream} that will consume any values signaled by this {@code Stream}. As
	 * such this a terminal action to be placed on a stream flow. Any Error signal will be consumed by the error
	 * consumer. The Complete signal will be consumed by the complete consumer. Only error and complete signal will be
	 * signaled downstream. It will also eagerly prefetch upstream publisher. <p>
	 * @param consumer the consumer to invoke on each value
	 * @param errorConsumer the consumer to invoke on each error signal
	 * @param completeConsumer the consumer to invoke on complete signal
	 * @return {@literal new Stream}
	 */
	public final DemandControl consumeLater(
			final Consumer<? super O> consumer,
			Consumer<? super Throwable> errorConsumer,
			Consumer<Void> completeConsumer) {
		ManualSubscriber<O> consumerAction = new ManualSubscriber<>(consumer, errorConsumer, completeConsumer);
		subscribe(consumerAction);
		return consumerAction;
	}

	/**
	 * Instruct the stream to request the produced subscription indefinitely. If the dispatcher is asynchronous
	 * (RingBufferDispatcher for instance), it will proceed the request asynchronously as well.
	 * @return the consuming action
	 */
	@SuppressWarnings("unchecked")
	public Control consume() {
		return consume(NOOP);
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will consume any values accepted by this {@code Stream}. As
	 * such this a terminal action to be placed on a stream flow. It will also eagerly prefetch upstream publisher. <p>
	 * For a passive version that observe and forward incoming data see {@link #observe(reactor.fn.Consumer)}
	 * @param consumer the consumer to invoke on each value
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public final Control consume(final Consumer<? super O> consumer) {
		long c = Math.min(Integer.MAX_VALUE, getCapacity());
		InterruptableSubscriber<O> consumerAction;
		if(c == Integer.MAX_VALUE){
			consumerAction = new InterruptableSubscriber<O>(consumer, null, null);
		}
		else{
			consumerAction = new BoundedSubscriber<O>((int)c, consumer, null, null);
		}
		subscribe(consumerAction);
		return consumerAction;
	}

	/**
	 * Attach 2 {@link Consumer} to this {@code Stream} that will consume any values signaled by this {@code Stream}. As
	 * such this a terminal action to be placed on a stream flow. Any Error signal will be consumed by the error
	 * consumer. It will also eagerly prefetch upstream publisher. <p>
	 * @param consumer the consumer to invoke on each next signal
	 * @param errorConsumer the consumer to invoke on each error signal
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public final Control consume(final Consumer<? super O> consumer, Consumer<? super Throwable> errorConsumer) {
		return consume(consumer, errorConsumer, null);
	}

	/**
	 * Attach 3 {@link Consumer} to this {@code Stream} that will consume any values signaled by this {@code Stream}. As
	 * such this a terminal action to be placed on a stream flow. Any Error signal will be consumed by the error
	 * consumer. The Complete signal will be consumed by the complete consumer. Only error and complete signal will be
	 * signaled downstream. It will also eagerly prefetch upstream publisher. <p>
	 * @param consumer the consumer to invoke on each value
	 * @param errorConsumer the consumer to invoke on each error signal
	 * @param completeConsumer the consumer to invoke on complete signal
	 * @return {@literal new Stream}
	 */
	public final Control consume(final Consumer<? super O> consumer,
			Consumer<? super Throwable> errorConsumer,
			Consumer<Void> completeConsumer) {

		long c = Math.min(Integer.MAX_VALUE, getCapacity());

		InterruptableSubscriber<O> consumerAction;
		if(c == Integer.MAX_VALUE){
			consumerAction = new InterruptableSubscriber<O>(consumer, errorConsumer, completeConsumer);
		}
		else{
			consumerAction = new BoundedSubscriber<O>((int)c, consumer, errorConsumer, completeConsumer);
		}

		subscribe(consumerAction);
		return consumerAction;
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will consume any values accepted by this {@code Stream}. As
	 * such this a terminal action to be placed on a stream flow. It will also eagerly prefetch upstream publisher. <p>
	 * The passed {code requestMapper} function will receive the {@link Stream} of the last N requested elements
	 * -starting with the capacity defined for the stream- when the N elements have been consumed. It will return a
	 * {@link Publisher} of long signals S that will instruct the consumer to request S more elements, possibly altering
	 * the "batch" size if wished. <p> <p> For a passive version that observe and forward incoming data see {@link
	 * #observe(reactor.fn.Consumer)}
	 * @param consumer the consumer to invoke on each value
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public final Control consumeWithRequest(final Consumer<? super O> consumer,
			final Function<Long, ? extends Long> requestMapper) {
		return consumeWhen(consumer, new Function<Stream<Long>, Publisher<? extends Long>>() {
			@Override
			public Publisher<? extends Long> apply(Stream<Long> longStream) {
				return longStream.map(requestMapper);
			}
		});
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will consume any values accepted by this {@code Stream}. As
	 * such this a terminal action to be placed on a stream flow. It will also eagerly prefetch upstream publisher. <p>
	 * The passed {code requestMapper} function will receive the {@link Stream} of the last N requested elements
	 * -starting with the capacity defined for the stream- when the N elements have been consumed. It will return a
	 * {@link Publisher} of long signals S that will instruct the consumer to request S more elements. <p> For a passive
	 * version that observe and forward incoming data see {@link #observe(reactor.fn.Consumer)}
	 * @param consumer the consumer to invoke on each value
	 * @return a new {@link Control} interface to operate on the materialized upstream
	 */
	public final Control consumeWhen(final Consumer<? super O> consumer,
			final Function<Stream<Long>, ? extends Publisher<? extends Long>> requestMapper) {
		AdaptiveSubscriber<O> consumerAction =
				new AdaptiveSubscriber<O>(getTimer(), consumer, requestMapper);

		subscribe(consumerAction);
		return consumerAction;
	}


	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will observe any values accepted by this {@code Stream}.
	 * @param consumer the consumer to invoke on each value
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final Stream<O> observe(@Nonnull final Consumer<? super O> consumer) {
		return lift(new CallbackOperator<O>(consumer, null));
	}

	/**
	 * Cache last {@link BaseProcessor#SMALL_BUFFER_SIZE} signal to this {@code Stream} and release them on request that
	 * will observe any values accepted by this {@code Stream}.
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final Stream<O> cache() {
		return cache(BaseProcessor.SMALL_BUFFER_SIZE);
	}

	/**
	 * Cache all signal to this {@code Stream} and release them on request that will observe any values accepted by this
	 * {@code Stream}.
	 * @param last number of events retained in history
	 * @return {@literal new Stream}
	 * @since 2.1
	 */
	public final Stream<O> cache(int last) {
		Processor<O, O> emitter = Processors.replay(last);
		subscribe(emitter);
		return StreamProcessor.wrap(emitter);
	}

	/**
	 * Attach a {@link reactor.core.support.Logger} to this {@code Stream} that will observe any signal emitted.
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final Stream<O> log() {
		return log(null, LogOperator.ALL);
	}

	/**
	 * Attach a {@link reactor.core.support.Logger} to this {@code Stream} that will observe any signal emitted.
	 * @param category The logger name
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final Stream<O> log(String category) {
		return log(category, LogOperator.ALL);
	}

	/**
	 * Attach a {@link reactor.core.support.Logger} to this {@code Stream} that will observe any signal emitted.
	 * @param category The logger name
	 * @param options the bitwise checked flags for observed signals
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final Stream<O> log(final String category, int options) {
		return log(category, Level.INFO, options);
	}

	/**
	 * Attach a {@link reactor.core.support.Logger} to this {@code Stream} that will observe any signal emitted.
	 * @param category The logger name
	 * @param level The logger level
	 * @param options the bitwise checked flags for observed signals
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final Stream<O> log(final String category, Level level, int options) {
		return lift(new LogOperator<O>(category, level, options));
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will observe any complete signal
	 * @param consumer the consumer to invoke on complete
	 * @return {@literal a new stream}
	 * @since 2.0
	 */
	public final Stream<O> observeComplete(@Nonnull final Consumer<Void> consumer) {
		return lift(new CallbackOperator<O>(null, consumer));
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will observe any onSubscribe signal
	 * @param consumer the consumer to invoke on onSubscribe
	 * @return {@literal a new stream}
	 * @since 2.0
	 */
	public final Stream<O> observeStart(@Nonnull final Consumer<? super Subscription> consumer) {
		return lift(new StreamStateCallbackOperator<O>(null, consumer));
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will observe any cancel signal
	 * @param consumer the consumer to invoke on cancel
	 * @return {@literal a new stream}
	 * @since 2.0
	 */
	public final Stream<O> observeCancel(@Nonnull final Consumer<Void> consumer) {
		return lift(new StreamStateCallbackOperator<O>(consumer, null));
	}

	/**
	 * Attach a {@link Consumer} to this {@code Stream} that will observe terminal signal complete|error. The consumer
	 * will listen for the signal and introspect its state.
	 * @param consumer the consumer to invoke on terminal signal
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final Stream<O> finallyDo(final Consumer<Signal<O>> consumer) {
		return lift(new FinallyOperator<O>(consumer));
	}

	/**
	 * Create an operation that returns the passed value if the Stream has completed without any emitted signals.
	 * @param defaultValue the value to forward if the stream is empty
	 * @return {@literal new Stream}
	 * @since 2.0
	 */
	public final Stream<O> defaultIfEmpty(final O defaultValue) {
		return lift(new DefaultIfEmptyOperator<O>(defaultValue));
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code V} and pass it into
	 * another {@code Stream}.
	 * @param fn the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@link Stream} containing the transformed values
	 */
	public final <V> Stream<V> map(@Nonnull final Function<? super O, ? extends V> fn) {
		return lift(new MapOperator<O, V>(fn));
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code Stream<O,V>} and pass
	 * it into another {@code Stream}.
	 * @param fn the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@link Stream} containing the transformed values
	 * @since 2.1
	 */
	public final <V> Stream<V> forkJoin(final int concurrency,
			@Nonnull final Function<GroupedStream<Integer, O>, Publisher<V>> fn) {
		Assert.isTrue(concurrency > 0, "Must subscribe once at least, concurrency set to " + concurrency);

		Publisher<V> pub;
		final List<Publisher<? extends V>> publisherList = new ArrayList<>(concurrency);

		for (int i = 0; i < concurrency; i++) {
			pub = fn.apply(new GroupedStream<Integer, O>(i) {
				@Override
				public void subscribe(Subscriber<? super O> s) {
					Stream.this.subscribe(s);
				}

				@Override
				public long getCapacity() {
					return Stream.this.getCapacity();
				}

				@Override
				public Timer getTimer() {
					return Stream.this.getTimer();
				}
			});

			if (concurrency == 1) {
				return Streams.wrap(pub);
			}
			else {
				publisherList.add(pub);
			}
		}

		final Publisher<V> mergedStream = Publishers.merge(Publishers.from(publisherList));

		return new Lift<O, V>(this) {
			@Override
			public void subscribe(Subscriber<? super V> s) {
				mergedStream.subscribe(s);
			}

			@Override
			public String getName() {
				return "forkJoin";
			}
		};
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code Stream<O,V>} and pass
	 * it into another {@code Stream}.
	 * @param fn the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@link Stream} containing the transformed values
	 * @since 1.1, 2.0
	 */
	public final <V> Stream<V> flatMap(@Nonnull final Function<? super O, ? extends Publisher<? extends V>> fn) {

		return new Lift<O, V>(this) {
			@Override
			public void subscribe(Subscriber<? super V> s) {
				Publishers.flatMap(Stream.this, fn)
				          .subscribe(s);
			}

			@Override
			public String getName() {
				return "flatMap";
			}
		};
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code Stream<O,V>} and pass
	 * it into another {@code Stream}. The produced stream will emit the data from the most recent transformed stream.
	 * @param fn the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@link Stream} containing the transformed values
	 * @since 1.1, 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <V> Stream<V> switchMap(@Nonnull final Function<? super O, Publisher<? extends V>> fn) {
		return map(fn).lift(SwitchOperator.INSTANCE);
	}

	/**
	 * Assign the given {@link Function} to transform the incoming value {@code T} into a {@code Stream<O,V>} and pass
	 * it into another {@code Stream}. The produced stream will emit the data from all transformed streams in order.
	 * @param fn the transformation function
	 * @param <V> the type of the return value of the transformation function
	 * @return a new {@link Stream} containing the transformed values
	 * @since 1.1, 2.0
	 */
	public final <V> Stream<V> concatMap(@Nonnull final Function<? super O, Publisher<? extends V>> fn) {
		return new Lift<O, V>(this) {
			@Override
			public void subscribe(Subscriber<? super V> s) {
				Publishers.concatMap(Stream.this, fn)
				          .subscribe(s);
			}

			@Override
			public String getName() {
				return "concatMap";
			}
		};
	}

	/**
	 * Pass all the nested {@link Publisher} values to a new {@link Stream}. Dynamic merge requires use of
	 * reactive-pull offered by default StreamSubscription. If merge hasn't getCapacity() to take new elements because
	 * its {@link #getCapacity()(long)} instructed so, the subscription will buffer them.
	 * @param <V> the inner stream flowing data type that will be the produced signal.
	 * @return the merged stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <V> Stream<V> merge() {
		final Stream<? extends Publisher<? extends V>> thiz = (Stream<? extends Publisher<? extends V>>) this;

		return new Lift<O, V>(this) {
			@Override
			public void subscribe(Subscriber<? super V> s) {
				Publishers.merge(thiz)
				          .subscribe(s);
			}

			@Override
			public String getName() {
				return "merge";
			}
		};
	}

	/**
	 * Pass all the nested {@link Publisher} values from this current upstream and from the passed publisher.
	 * @return the merged stream
	 * @since 2.0
	 */
	public final Stream<O> mergeWith(final Publisher<? extends O> publisher) {
		return new Lift<O, O>(this) {
			@Override
			public void subscribe(Subscriber<? super O> s) {
				Publishers.merge(Publishers.from(Arrays.asList(Stream.this, publisher)))
				          .subscribe(s);
			}

			@Override
			public String getName() {
				return "mergeWith";
			}
		};
	}

	/**
	 * Pass all the nested {@link Publisher} values from this current upstream and then on complete consume from
	 * the passed publisher.
	 * @return the merged stream
	 * @since 2.0
	 */
	public final Stream<O> concatWith(final Publisher<? extends O> publisher) {
		return new Lift<O, O>(this) {
			@Override
			public void subscribe(Subscriber<? super O> s) {
				Publishers.concat(Publishers.from(Arrays.asList(Stream.this, publisher)))
				          .subscribe(s);
			}

			@Override
			public String getName() {
				return "concatWith";
			}
		};
	}

	/**
	 * Start emitting all items from the passed publisher then emits from the current stream.
	 * @return the merged stream
	 * @since 2.0
	 */
	public final Stream<O> startWith(final Iterable<O> iterable) {
		return startWith(Streams.from(iterable));
	}

	/**
	 * Start emitting all items from the passed publisher then emits from the current stream.
	 * @return the merged stream
	 * @since 2.0
	 */
	public final Stream<O> startWith(final O value) {
		return startWith(Streams.just(value));
	}

	/**
	 * Start emitting all items from the passed publisher then emits from the current stream.
	 * @return the merged stream
	 * @since 2.0
	 */
	public final Stream<O> startWith(final Publisher<? extends O> publisher) {
		if (publisher == null) {
			return this;
		}
		return Streams.concat(publisher, this);
	}

	/**
	 * Pass all the nested {@link Publisher} values to a new {@link Stream} until one of them complete. The
	 * result will be produced with a list of each upstream most recent emitted data.
	 * @return the zipped and joined stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <T> Stream<List<T>> join() {
		return zip((Function<Tuple, List<T>>) ZipOperator.JOIN_FUNCTION);
	}

	/**
	 * Pass all the nested {@link Publisher} values to a new {@link Stream} until one of them complete. The
	 * result will be produced with a list of each upstream most recent emitted data.
	 * @return the zipped and joined stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <T> Stream<List<T>> joinWith(Publisher<T> publisher) {
		return zipWith(publisher, (BiFunction<Object, Object, List<T>>) ZipOperator.JOIN_BIFUNCTION);
	}

	/**
	 * Pass all the nested {@link Publisher} values to a new {@link Stream} until one of them complete. The
	 * result will be produced by the zipper transformation from a tuple of each upstream most recent emitted data.
	 * @return the merged stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <V> Stream<V> zip(final @Nonnull Function<? super TupleN, ? extends V> zipper) {
		final Stream<Publisher<?>> thiz = (Stream<Publisher<?>>) this;

		return thiz.buffer().map(new Function<List<Publisher<?>>, Publisher[]>() {
			@Override
			public Publisher[] apply(List<Publisher<?>> publishers) {
				return publishers.toArray(new Publisher[publishers.size()]);
			}
		}).lift(new ZipOperator<>(zipper, BaseProcessor.XS_BUFFER_SIZE));
	}

	/**
	 * Pass all the nested {@link Publisher} values to a new {@link Stream} until one of them complete. The
	 * result will be produced by the zipper transformation from a tuple of each upstream most recent emitted data.
	 * @return the zipped stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final <T2, V> Stream<V> zipWith(Iterable<? extends T2> iterable,
			@Nonnull BiFunction<? super O, ? super T2, ? extends V>  zipper) {
		return zipWithIterable(iterable, zipper);
	}

	/**
	 * Pass with the passed {@link Publisher} values to a new {@link Stream} until one of them complete. The
	 * result will be produced by the zipper transformation from a tuple of each upstream most recent emitted data.
	 * @return the zipped stream
	 * @since 2.0
	 */
	public final <T2, V> Stream<V> zipWith(final Publisher<? extends T2> publisher,
			final @Nonnull BiFunction<? super O, ? super T2, ? extends V> zipper) {
		return new Lift<O, V>(this) {
			@Override
			public void subscribe(Subscriber<? super V> s) {
				Publishers.<O, T2, V>zip(Stream.this, publisher, zipper).subscribe(s);
			}

			@Override
			public String getName() {
				return "zipWith";
			}
		};
	}

	/**
	 * Pass all the nested {@link Publisher} values to a new {@link Stream} until one of them complete. The
	 * result will be produced by the zipper transformation from a tuple of each upstream most recent emitted data.
	 * @return the zipped stream
	 * @since 2.1
	 */
	public final <T2, V> Stream<V> zipWithIterable(Iterable<? extends T2> iterable,
			@Nonnull BiFunction<? super O, ? super T2, ? extends V> zipper) {
		return lift(new ZipWithIterableOperator<>(zipper, iterable));
	}

	/**
	 * Bind the stream to a given {@param elements} volume of in-flight data: - A {@link Subscriber} will request up to the
	 * defined volume upstream. - a
	 * {@link Subscriber} will track the pending requests and fire up to {@param elements}
	 * when the previous volume has been processed. - A {@link BatchOperator} and any other size-bound action will be
	 * limited to the defined volume. <p> <p> A stream capacity can't be superior to the underlying dispatcher capacity:
	 * if the {@param elements} overflow the dispatcher backlog size, the capacity will be aligned automatically to fit
	 * it. RingBufferDispatcher will for instance take to a power of 2 size up to {@literal Integer.MAX_VALUE}, where a
	 * Stream can be sized up to {@literal Long.MAX_VALUE} in flight data. <p> <p> When the stream receives more
	 * elements than requested, incoming data is eventually staged in a {@link org.reactivestreams.Subscription}. The
	 * subscription can react differently according to the implementation in-use, the default strategy is as following:
	 * - The first-level of pair compositions Stream->Subscriber will overflow data in a {@link java.util.Queue}, ready to
	 * be polled when the action fire the pending requests. - The following pairs of Subscriber->Subscriber will synchronously
	 * pass data - Any pair of Stream->Subscriber or Subscriber->Subscriber will behave as with the root Stream->Action pair
	 * rule. - {@link #onOverflowBuffer()} force this staging behavior, with a possibilty to pass a {@link
	 * reactor.core.queue .PersistentQueue}
	 * @param elements maximum number of in-flight data
	 * @return a backpressure capable stream
	 */
	public Stream<O> capacity(final long elements) {
		if (elements == getCapacity()) {
			return this;
		}

		return new Lift<O, O>(this){
			@Override
			public long getCapacity() {
				return elements;
			}

			@Override
			public String getName() {
				return "capacitySetup";
			}
		};
	}

	/**
	 * Make this Stream subscribers unbounded
	 * @return Stream with capacity set to max
	 * @see #capacity(long)
	 */
	public final Stream<O> unbounded() {
		return capacity(Long.MAX_VALUE);
	}

	/**
	 * Attach a No-Op Stream that only serves the purpose of buffering incoming values if not enough demand is signaled
	 * downstream. A buffering capable stream will prevent underlying dispatcher to be saturated (and sometimes
	 * blocking).
	 * @return a buffered stream
	 * @since 2.0
	 */
	public final Stream<O> onOverflowBuffer() {
	    return onOverflowBuffer(BaseProcessor.SMALL_BUFFER_SIZE);
	}

	/**
	 * Attach a No-Op Stream that only serves the purpose of buffering incoming values if not enough demand is signaled
	 * downstream. A buffering capable stream will prevent underlying dispatcher to be saturated (and sometimes
	 * blocking).
	 * @param size max buffer size
	 * @return a buffered stream
	 * @since 2.0
	 */
	public final Stream<O> onOverflowBuffer(final int size) {
		return new Lift<O,O>(this) {
			@Override
			public void subscribe(Subscriber<? super O> s) {
				Processor<O, O> emitter = Processors.replay(size);
				emitter.subscribe(s);
				Stream.this.subscribe(emitter);
			}

			@Override
			public String getName() {
				return "onOverflowBuffer";
			}
		};
	}

	/**
	 * Attach a No-Op Stream that only serves the purpose of dropping incoming values if not enough demand is signaled
	 * downstream. A dropping stream will prevent underlying dispatcher to be saturated (and sometimes blocking).
	 * @return a dropping stream
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final Stream<O> onOverflowDrop() {
		return lift(DropOperator.INSTANCE);
	}

	/**
	 * Evaluate each accepted value against the given {@link Predicate}. If the predicate test succeeds, the value is
	 * passed into the new {@code Stream}. If the predicate test fails, the value is ignored.
	 * @param p the {@link Predicate} to test values against
	 * @return a new {@link Stream} containing only values that pass the predicate test
	 */
	public final Stream<O> filter(final Predicate<? super O> p) {
		return lift(new FilterOperator<>(p));
	}

	/**
	 * Evaluate each accepted boolean value. If the predicate test succeeds, the value is passed into the new {@code
	 * Stream}. If the predicate test fails, the value is ignored.
	 * @return a new {@link Stream} containing only values that pass the predicate test
	 * @since 1.1, 2.0
	 */
	@SuppressWarnings("unchecked")
	public final Stream<Boolean> filter() {
		return ((Stream<Boolean>) this).filter(FilterOperator.simplePredicate);
	}

	/**
	 * Create a new {@code Stream} whose only value will be the current instance of the {@link Stream}.
	 * @return a new {@link Stream} whose only value will be the materialized current {@link Stream}
	 * @since 2.0
	 */
	public final Stream<Stream<O>> nest() {
		return Streams.just(this);
	}

	/**
	 * Create a new {@code Stream} which will re-subscribe its oldest parent-child stream pair. The action will start
	 * propagating errors after {@literal Integer.MAX_VALUE}.
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> retry() {
		return retry(-1);
	}

	/**
	 * Create a new {@code Stream} which will re-subscribe its oldest parent-child stream pair. The action will start
	 * propagating errors after {@param numRetries}. This is generally useful for retry strategies and fault-tolerant
	 * streams.
	 * @param numRetries the number of times to tolerate an error
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> retry(int numRetries) {
		return retry(numRetries, null);
	}

	/**
	 * Create a new {@code Stream} which will re-subscribe its oldest parent-child stream pair. {@param retryMatcher}
	 * will test an incoming {@link Throwable}, if positive the retry will occur. This is generally useful for retry
	 * strategies and fault-tolerant streams.
	 * @param retryMatcher the predicate to evaluate if retry should occur based on a given error signal
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> retry(Predicate<Throwable> retryMatcher) {
		return retry(-1, retryMatcher);
	}

	/**
	 * Create a new {@code Stream} which will re-subscribe its oldest parent-child stream pair. The action will start
	 * propagating errors after {@param numRetries}. {@param retryMatcher} will test an incoming {@Throwable}, if
	 * positive the retry will occur (in conjonction with the {@param numRetries} condition). This is generally useful
	 * for retry strategies and fault-tolerant streams.
	 * @param numRetries the number of times to tolerate an error
	 * @param retryMatcher the predicate to evaluate if retry should occur based on a given error signal
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> retry(final int numRetries, final Predicate<Throwable> retryMatcher) {
		return lift(new RetryOperator<O>(numRetries, retryMatcher, this));
	}

	/**
	 * Create a new {@code Stream} which will re-subscribe its oldest parent-child stream pair if the backOff stream
	 * produced by the passed mapper emits any next data or complete signal. It will propagate the error if the backOff
	 * stream emits an error signal.
	 * @param backOffStream the function taking the error stream as an downstream and returning a new stream that
	 * applies some backoff policy e.g. Streams.timer
	 * @return a new fault-tolerant {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> retryWhen(final Function<? super Stream<? extends Throwable>, ? extends Publisher<?>> backOffStream) {
		return lift(new RetryWhenOperator<O>(getTimer(), backOffStream, this));
	}

	/**
	 * Create a new {@code Stream} which will keep re-subscribing its oldest parent-child stream pair on complete.
	 * @return a new infinitely repeated {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> repeat() {
		return repeat(-1);
	}

	/**
	 * Create a new {@code Stream} which will keep re-subscribing its oldest parent-child stream pair on complete. The
	 * action will be propagating complete after {@param numRepeat}. if positive
	 * @param numRepeat the number of times to re-subscribe on complete
	 * @return a new repeated {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> repeat(final int numRepeat) {
		return lift(new RepeatOperator<O>(numRepeat, this));
	}

	/**
	 * Create a new {@code Stream} which will re-subscribe its oldest parent-child stream pair if the backOff stream
	 * produced by the passed mapper emits any next signal. It will propagate the complete and error if the backoff
	 * stream emits the relative signals.
	 * @param backOffStream the function taking a stream of complete timestamp in millis as an downstream and returning
	 * a new stream that applies some backoff policy, e.g. @{link Streams#timer(long)}
	 * @return a new repeated {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> repeatWhen(final Function<? super Stream<? extends Long>, ? extends Publisher<?>> backOffStream) {
		return lift(new RepeatWhenOperator<O>(getTimer(), backOffStream, this));
	}

	/**
	 * Create a new {@code Stream} that will signal the last element observed before complete signal.
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final Stream<O> last() {
		return lift(LastOperator.INSTANCE);
	}

	/**
	 * Create a new {@code Stream} that will signal next elements up to {@param max} times.
	 * @param max the number of times to broadcast next signals before completing
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> take(final long max) {
		return lift(new TakeOperator<O>(max));
	}

	/**
	 * Create a new {@code Stream} that will signal next elements up to the specified {@param time}.
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
	 * @param time the time window to broadcast next signals before completing
	 * @param unit the time unit to use
	 * @param timer the Timer to use
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> take(final long time, final TimeUnit unit, final Timer timer) {
		if (time > 0) {
			Assert.isTrue(timer != null, "Timer can't be found, try assigning an environment to the stream");
			return lift(new TakeUntilTimeoutOperator<O>(time, unit, timer));
		}
		else {
			return Streams.empty();
		}
	}

	/**
	 * Create a new {@code Stream} that will signal next elements while {@param limitMatcher} is true.
	 * @param limitMatcher the predicate to evaluate for starting dropping events and completing
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> takeWhile(final Predicate<O> limitMatcher) {
		return lift(new TakeWhileOperator<O>(limitMatcher));
	}

	/**
	 * Create a new {@code Stream} that will NOT signal next elements up to {@param max} times.
	 * @param max the number of times to drop next signals before starting
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> skip(long max) {
		return skipWhile(max, null);
	}

	/**
	 * Create a new {@code Stream} that will NOT signal next elements up to the specified {@param time}.
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
	 * @param time the time window to drop next signals before starting
	 * @param unit the time unit to use
	 * @param timer the Timer to use
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> skip(final long time, final TimeUnit unit, final Timer timer) {
		if (time > 0) {
			Assert.isTrue(timer != null, "Timer can't be found, try assigning an environment to the stream");
			return lift(new SkipUntilTimeoutOperator<O>(time, unit, timer));
		}
		else {
			return this;
		}
	}

	/**
	 * Create a new {@code Stream} that will NOT signal next elements while {@param limitMatcher} is true.
	 * @param limitMatcher the predicate to evaluate to start broadcasting events
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> skipWhile(Predicate<O> limitMatcher) {
		return skipWhile(Long.MAX_VALUE, limitMatcher);
	}

	/**
	 * Create a new {@code Stream} that will NOT signal next elements while {@param limitMatcher} is true or up to
	 * {@param max} times.
	 * @param max the number of times to drop next signals before starting
	 * @param limitMatcher the predicate to evaluate for starting dropping events and completing
	 * @return a new limited {@code Stream}
	 * @since 2.0
	 */
	public final Stream<O> skipWhile(final long max, final Predicate<O> limitMatcher) {
		if (max > 0) {
			return lift(new SkipOperator<O>(limitMatcher, max));
		}
		else {
			return this;
		}
	}

	/**
	 * Create a new {@code Stream} that accepts a {@link reactor.fn.tuple.Tuple2} of T1 {@link Long} system time in
	 * millis and T2 {@link <T>} associated data
	 * @return a new {@link Stream} that emits tuples of millis time and matching data
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final Stream<Tuple2<Long, O>> timestamp() {
		return lift(MapOperator.<O>timestamp());
	}

	/**
	 * Create a new {@code Stream} that accepts a {@link reactor.fn.tuple.Tuple2} of T1 {@link Long} timemillis and T2
	 * {@link <T>} associated data. The timemillis corresponds to the elapsed time between the subscribe and the first
	 * next signal OR between two next signals.
	 * @return a new {@link Stream} that emits tuples of time elapsed in milliseconds and matching data
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final Stream<Tuple2<Long, O>> elapsed() {
		return lift(ElapsedOperator.INSTANCE);
	}

	/**
	 * Create a new {@code Stream} that emits an item at a specified index from a source {@code Stream}
	 * @param index index of an item
	 * @return a source item at a specified index
	 */
	public final Stream<O> elementAt(final int index) {
		return lift(new ElementAtOperator<O>(index));
	}

	/**
	 * Create a new {@code Stream} that emits an item at a specified index from a source {@code Stream} or default value
	 * when index is out of bounds
	 * @param index index of an item
	 * @return a source item at a specified index or a default value
	 */
	public final Stream<O> elementAtOrDefault(final int index, final O defaultValue) {
		return lift(new ElementAtOperator<O>(index, defaultValue));
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch. Requires a {@code
	 * getCapacity()} to have been set. <p> When a new batch is triggered, the first value of that next batch will be
	 * pushed into this {@code Stream}.
	 * @return a new {@link Stream} whose values are the first value of each batch
	 */
	public final Stream<O> sampleFirst() {
		return sampleFirst((int) Math.min(Integer.MAX_VALUE, getCapacity()));
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch. <p> When a new batch is
	 * triggered, the first value of that next batch will be pushed into this {@code Stream}.
	 * @param batchSize the batch size to use
	 * @return a new {@link Stream} whose values are the first value of each batch)
	 */
	public final Stream<O> sampleFirst(final int batchSize) {
		return lift(new SampleOperator<O>(batchSize, true));
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch.
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit the time unit
	 * @return a new {@link Stream} whose values are the first value of each batch
	 */
	public final Stream<O> sampleFirst(long timespan, TimeUnit unit) {
		return sampleFirst(Integer.MAX_VALUE, timespan, unit);
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch.
	 * @param maxSize the max counted size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit the time unit
	 * @return a new {@link Stream} whose values are the first value of each batch
	 */
	public final Stream<O> sampleFirst(int maxSize, long timespan, TimeUnit unit) {
		return sampleFirst(maxSize, timespan, unit, getTimer());
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch.
	 * @param maxSize the max counted size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit the time unit
	 * @param timer the Timer to run on
	 * @return a new {@link Stream} whose values are the first value of each batch
	 */
	public final Stream<O> sampleFirst(final int maxSize, final long timespan, final TimeUnit unit, final Timer timer) {
		return lift(new SampleOperator<O>(true, maxSize, timespan, unit, timer));
	}

	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code
	 * getCapacity()}
	 * @return a new {@link Stream} whose values are the last value of each batch
	 */
	public final Stream<O> sample() {
		return sample((int) Math.min(Integer.MAX_VALUE, getCapacity()));
	}

	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code
	 * getCapacity()}
	 * @param batchSize the batch size to use
	 * @return a new {@link Stream} whose values are the last value of each batch
	 */
	public final Stream<O> sample(final int batchSize) {
		return lift(new SampleOperator<O>(batchSize));
	}

	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch.
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit the time unit
	 * @return a new {@link Stream} whose values are the last value of each batch
	 */
	public final Stream<O> sample(long timespan, TimeUnit unit) {
		return sample(Integer.MAX_VALUE, timespan, unit, getTimer());
	}

	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch.
	 * @param maxSize the max counted size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit the time unit
	 * @return a new {@link Stream} whose values are the last value of each batch
	 */
	public final Stream<O> sample(int maxSize, long timespan, TimeUnit unit) {
		return sample(maxSize, timespan, unit, getTimer());
	}

	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch.
	 * @param maxSize the max counted size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit the time unit
	 * @param timer the Timer to run on
	 * @return a new {@link Stream} whose values are the last value of each batch
	 */
	public final Stream<O> sample(final int maxSize, final long timespan, final TimeUnit unit, final Timer timer) {
		return lift(new SampleOperator<O>(false, maxSize, timespan, unit, timer));
	}

	/**
	 * Create a new {@code Stream} that filters out consecutive equals values.
	 * @return a new {@link Stream} whose values are the last value of each batch
	 * @since 2.0
	 */
	public final Stream<O> distinctUntilChanged() {
		return lift(new DistinctUntilChangedOperator<O, O>(null));
	}

	/**
	 * Create a new {@code Stream} that filters out consecutive values having equal keys computed by function
	 * @param keySelector function to compute comparison key for each element
	 * @return a new {@link Stream} whose values are the last value of each batch
	 * @since 2.0
	 */
	public final <V> Stream<O> distinctUntilChanged(final Function<? super O, ? extends V> keySelector) {
		return lift(new DistinctUntilChangedOperator<>(keySelector));
	}

	/**
	 * Create a new {@code Stream} that filters in only unique values.
	 * @return a new {@link Stream} with unique values
	 */
	public final Stream<O> distinct() {
		return lift(new DistinctOperator<O, O>(null));
	}

	/**
	 * Create a new {@code Stream} that filters in only values having distinct keys computed by function
	 * @param keySelector function to compute comparison key for each element
	 * @return a new {@link Stream} with values having distinct keys
	 */
	public final <V> Stream<O> distinct(final Function<? super O, ? extends V> keySelector) {
		return lift(new DistinctOperator<>(keySelector));
	}

	/**
	 * Create a new {@code Stream} that emits <code>true</code> when any value satisfies a predicate and
	 * <code>false</code> otherwise
	 * @param predicate predicate tested upon values
	 * @return a new {@link Stream} with <code>true</code> if any value satisfies a predicate and <code>false</code>
	 * otherwise
	 * @since 2.0
	 */
	public final Stream<Boolean> exists(final Predicate<? super O> predicate) {
		return lift(new ExistsOperator<O>(predicate));
	}

	/**
	 * Collect incoming values into a {@link java.util.List} that will be pushed into the returned {@code Stream} every
	 * time {@link #getCapacity()} has been reached, or flush is triggered.
	 * @return a new {@link Stream} whose values are a {@link java.util.List} of all values in this batch
	 */
	public final Stream<List<O>> buffer() {
		return buffer((int) Math.min(Integer.MAX_VALUE, getCapacity()));
	}

	/**
	 * Collect incoming values into multiple {@link List} buckets that will be pushed into the returned {@code Stream}
	 * every time {@link #getCapacity()} has been reached.
	 * @param maxSize the collected size
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(final int maxSize) {
		return lift(new BufferOperator<O>(maxSize));
	}

	/**
	 * Collect incoming values into a {@link List} that will be moved into the returned {@code Stream} every time the
	 * passed boundary publisher emits an item. Complete will flush any remaining items.
	 * @param bucketOpening the publisher to subscribe to on start for creating new buffer on next or complete signals.
	 * @param boundarySupplier the factory to provide a publisher to subscribe to when a buffer has been started
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(final Publisher<?> bucketOpening,
			final Supplier<? extends Publisher<?>> boundarySupplier) {

		return lift(new BufferShiftWhenOperator<O>(bucketOpening, boundarySupplier));
	}

	/**
	 * Collect incoming values into a {@link List} that will be moved into the returned {@code Stream} every time the
	 * passed boundary publisher emits an item. Complete will flush any remaining items.
	 * @param boundarySupplier the factory to provide a publisher to subscribe to on start for emiting and starting a
	 * new buffer
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(final Supplier<? extends Publisher<?>> boundarySupplier) {
		return lift(new BufferWhenOperator<O>(boundarySupplier));
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time
	 * {@code maxSize} has been reached by any of them. Complete signal will flush any remaining buckets.
	 * @param skip the number of items to skip before creating a new bucket
	 * @param maxSize the collected size
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(final int maxSize, final int skip) {
		if (maxSize == skip) {
			return buffer(maxSize);
		}
		return lift(new BufferShiftOperator<O>(maxSize, skip));
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every timespan.
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit the time unit
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(long timespan, TimeUnit unit) {
		return buffer(timespan, unit, getTimer());
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every timespan.
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit the time unit
	 * @param timer the Timer to run on
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(long timespan, TimeUnit unit, Timer timer) {
		return buffer(Integer.MAX_VALUE, timespan, unit, timer);
	}

	/**
	 * Collect incoming values into multiple {@link List} buckets created every {@code timeshift }that will be pushed
	 * into the returned {@code Stream} every timespan. Complete signal will flush any remaining buckets.
	 * @param timespan the period in unit to use to release buffered lists
	 * @param timeshift the period in unit to use to create a new bucket
	 * @param unit the time unit
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(final long timespan, final long timeshift, final TimeUnit unit) {
		return buffer(timespan, timeshift, unit, getTimer());
	}

	/**
	 * Collect incoming values into multiple {@link List} buckets created every {@code timeshift }that will be pushed
	 * into the returned {@code Stream} every timespan. Complete signal will flush any remaining buckets.
	 * @param timespan the period in unit to use to release buffered lists
	 * @param timeshift the period in unit to use to create a new bucket
	 * @param unit the time unit
	 * @param timer the Timer to run on
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(final long timespan,
			final long timeshift,
			final TimeUnit unit,
			final Timer timer) {
		if (timespan == timeshift) {
			return buffer(timespan, unit, timer);
		}
		return lift(new BufferShiftOperator<O>(Integer.MAX_VALUE, Integer.MAX_VALUE, timeshift, timespan, unit, timer));
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every timespan
	 * OR maxSize items.
	 * @param maxSize the max collected size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit the time unit
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(int maxSize, long timespan, TimeUnit unit) {
		return buffer(maxSize, timespan, unit, getTimer());
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every timespan
	 * OR maxSize items
	 * @param maxSize the max collected size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit the time unit
	 * @param timer the Timer to run on
	 * @return a new {@link Stream} whose values are a {@link List} of all values in this batch
	 */
	public final Stream<List<O>> buffer(final int maxSize,
			final long timespan,
			final TimeUnit unit,
			final Timer timer) {
		return lift(new BufferOperator<O>(maxSize, timespan, unit, timer));
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the
	 * returned fresh {@link Stream}. Possible flush triggers are: {@link #getCapacity()}, complete signal or request
	 * signal. PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 * @return a new {@link Stream} whose values re-ordered using a PriorityQueue.
	 * @since 2.0
	 */
	public final Stream<O> sort() {
		return sort(null);
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the
	 * returned fresh {@link Stream}. Possible flush triggers are: {@link #getCapacity()}, complete signal or request
	 * signal. PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 * @param maxCapacity a fixed maximum number or elements to re-order at once.
	 * @return a new {@link Stream} whose values re-ordered using a PriorityQueue.
	 * @since 2.0
	 */
	public final Stream<O> sort(int maxCapacity) {
		return sort(maxCapacity, null);
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the
	 * returned fresh {@link Stream}. Possible flush triggers are: {@link #getCapacity()}, complete signal or request
	 * signal. PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 * @param comparator A {@link Comparator<O>} to evaluate incoming data
	 * @return a new {@link Stream} whose values re-ordered using a PriorityQueue.
	 * @since 2.0
	 */
	public final Stream<O> sort(Comparator<? super O> comparator) {
		return sort((int) Math.min(Integer.MAX_VALUE, getCapacity()), comparator);
	}

	/**
	 * Stage incoming values into a {@link java.util.PriorityQueue<O>} that will be re-ordered and signaled to the
	 * returned fresh {@link Stream}. Possible flush triggers are: {@link #getCapacity()}, complete signal or request
	 * signal. PriorityQueue will use the {@link Comparable<O>} interface from an incoming data signal.
	 * @param maxCapacity a fixed maximum number or elements to re-order at once.
	 * @param comparator A {@link Comparator<O>} to evaluate incoming data
	 * @return a new {@link Stream} whose values re-ordered using a PriorityQueue.
	 * @since 2.0
	 */
	public final Stream<O> sort(final int maxCapacity, final Comparator<? super O> comparator) {
		return lift(new SortOperator<O>(maxCapacity, comparator));
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined {@link #getCapacity()}
	 * times. The nested streams will be pushed into the returned {@code Stream}.
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final Stream<Stream<O>> window() {
		return window((int) Math.min(Integer.MAX_VALUE, getCapacity()));
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined {@param backlog} times. The
	 * nested streams will be pushed into the returned {@code Stream}.
	 * @param backlog the time period when each window close and flush the attached consumer
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final Stream<Stream<O>> window(final int backlog) {
		return lift(new WindowOperator<O>(getTimer(), backlog));
	}

	/**
	 * Re-route incoming values into bucket streams that will be pushed into the returned {@code Stream} every {@code
	 * skip} and complete every time {@code maxSize} has been reached by any of them. Complete signal will flush any
	 * remaining buckets.
	 * @param skip the number of items to skip before creating a new bucket
	 * @param maxSize the collected size
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 */
	public final Stream<Stream<O>> window(final int maxSize, final int skip) {
		if (maxSize == skip) {
			return window(maxSize);
		}
		return lift(new WindowShiftOperator<O>(getTimer(), maxSize, skip));
	}

	/**
	 * Re-route incoming values into bucket streams that will be pushed into the returned {@code Stream} every  and
	 * complete every time {@code boundarySupplier} stream emits an item.
	 * @param boundarySupplier the factory to create the stream to listen to for separating each window
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 */
	public final Stream<Stream<O>> window(final Supplier<? extends Publisher<?>> boundarySupplier) {
		return lift(new WindowWhenOperator<O>(getTimer(), boundarySupplier));
	}

	/**
	 * Re-route incoming values into bucket streams that will be pushed into the returned {@code Stream} every and
	 * complete every time {@code boundarySupplier} stream emits an item. Window starts forwarding when the
	 * bucketOpening stream emits an item, then subscribe to the boundary supplied to complete.
	 * @param bucketOpening the publisher to listen for signals to create a new window
	 * @param boundarySupplier the factory to create the stream to listen to for closing an open window
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 */
	public final Stream<Stream<O>> window(final Publisher<?> bucketOpening,
			final Supplier<? extends Publisher<?>> boundarySupplier) {
		return lift(new WindowShiftWhenOperator<O>(getTimer(), bucketOpening, boundarySupplier));
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined timespan. The nested streams
	 * will be pushed into the returned {@code Stream}.
	 * @param timespan the period in unit to use to release a new window as a Stream
	 * @param unit the time unit
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final Stream<Stream<O>> window(long timespan, TimeUnit unit) {
		return window(Integer.MAX_VALUE, timespan, unit);
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} every pre-defined timespan OR maxSize items.
	 * The nested streams will be pushed into the returned {@code Stream}.
	 * @param maxSize the max collected size
	 * @param timespan the period in unit to use to release a buffered list
	 * @param unit the time unit
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final Stream<Stream<O>> window(final int maxSize, final long timespan, final TimeUnit unit) {
		return lift(new WindowOperator<O>(maxSize, timespan, unit, getTimer()));
	}

	/**
	 * Re-route incoming values into bucket streams that will be pushed into the returned {@code Stream} every {@code
	 * timeshift} period. These streams will complete every {@code timespan} period has cycled. Complete signal will
	 * flush any remaining buckets.
	 * @param timespan the period in unit to use to complete a window
	 * @param timeshift the period in unit to use to create a new window
	 * @param unit the time unit
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 */
	public final Stream<Stream<O>> window(final long timespan, final long timeshift, final TimeUnit unit) {
		if (timeshift == timespan) {
			return window(timespan, unit);
		}
		return lift(new WindowShiftOperator<O>(Integer.MAX_VALUE,
				Integer.MAX_VALUE,
				timespan,
				timeshift,
				unit,
				getTimer()));
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} for each unique key evaluated by the {param
	 * keyMapper}.
	 * @param keyMapper the key mapping function that evaluates an incoming data and returns a key.
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values in this window
	 * @since 2.0
	 */
	public final <K> Stream<GroupedStream<K, O>> groupBy(final Function<? super O, ? extends K> keyMapper) {
		return lift(new GroupByOperator<>(keyMapper, getTimer()));
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} for each unique key evaluated by the {param
	 * keyMapper}. The hashcode of the incoming data will be used for partitioning over {@link
	 * Processors#DEFAULT_POOL_SIZE} buckets. That means that at any point of time at most {@link
	 * Processors#DEFAULT_POOL_SIZE} number of streams will be created and used accordingly to the current hashcode % n
	 * result.
	 * @return a new {@link Stream} whose values are a {@link Stream} of all values routed to this partition
	 * @since 2.0
	 */
	public final Stream<GroupedStream<Integer, O>> partition() {
		return partition(Processors.DEFAULT_POOL_SIZE);
	}

	/**
	 * Re-route incoming values into a dynamically created {@link Stream} for each unique key evaluated by the {param
	 * keyMapper}. The hashcode of the incoming data will be used for partitioning over the buckets number passed. That
	 * means that at any point of time at most {@code buckets} number of streams will be created and used accordingly to
	 * the positive modulo of the current hashcode with respect to the number of buckets specified.
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
	 * Reduce the values passing through this {@code Stream} into an object {@code T}. This is a simple functional way
	 * for accumulating values. The arguments are the N-1 and N next signal in this order.
	 * @param fn the reduce function
	 * @return a new {@link Stream} whose values contain only the reduced objects
	 */
	public final Stream<O> reduce(@Nonnull final BiFunction<O, O, O> fn) {
		return scan(fn).last();
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}. The arguments are the N-1 and N
	 * next signal in this order.
	 * @param fn the reduce function
	 * @param initial the initial argument to pass to the reduce function
	 * @param <A> the type of the reduced object
	 * @return a new {@link Stream} whose values contain only the reduced objects
	 */
	public final <A> Stream<A> reduce(final A initial, @Nonnull BiFunction<A, ? super O, A> fn) {

		return scan(initial, fn).last();
	}

	/**
	 * Scan the values passing through this {@code Stream} into an object {@code A}. The arguments are the N-1 and N
	 * next signal in this order.
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
	 * @param initial the initial argument to pass to the reduce function
	 * @param fn the scan function
	 * @param <A> the type of the reduced object
	 * @return a new {@link Stream} whose values contain only the reduced objects
	 * @since 1.1, 2.0
	 */
	public final <A> Stream<A> scan(final A initial, final @Nonnull BiFunction<A, ? super O, A> fn) {
		return lift(new ScanOperator<O, A>(fn, initial));
	}

	/**
	 * Count accepted events for each batch and pass each accumulated long to the {@param stream}.
	 */
	public final Stream<Long> count() {
		return count(Long.MAX_VALUE);
	}

	/**
	 * Count accepted events for each batch {@param i} and pass each accumulated long to the {@param stream}.
	 * @return a new {@link Stream}
	 */
	@SuppressWarnings("unchecked")
	public final Stream<Long> count(final long i) {
		Stream<O> thiz = i != Long.MAX_VALUE ? take(i) : this;

		return thiz.lift(CountOperator.INSTANCE)
		           .last();
	}

	/**
	 * Request once the parent stream every {@param period} milliseconds. Timeout is run on the environment root timer.
	 * @param period the period in milliseconds between two notifications on this stream
	 * @return a new {@link Stream}
	 * @since 2.0
	 */
	public final Stream<O> throttle(final long period) {
		final Timer timer = getTimer();
		Assert.state(timer != null, "Cannot use default timer as no environment has been provided to this " + "Stream");

		return lift(new ThrottleRequestOperator<O>(timer, period));
	}

	/**
	 * Request the parent stream every time the passed throttleStream signals a Long request volume. Complete and Error
	 * signals will be propagated.
	 * @param throttleStream a function that takes a broadcasted stream of request signals and must return a stream of
	 * valid request signal (long).
	 * @return a new {@link Stream}
	 * @since 2.0
	 */
	public final Stream<O> requestWhen(final Function<? super Stream<? extends Long>, ? extends Publisher<? extends
			Long>> throttleStream) {
		return lift(new ThrottleRequestWhenOperator<O>(getTimer(), throttleStream));
	}

	/**
	 * Signal an error if no data has been emitted for {@param timeout} milliseconds. Timeout is run on the environment
	 * root timer. <p> A Timeout Exception will be signaled if no data or complete signal have been sent within the
	 * given period.
	 * @param timeout the timeout in milliseconds between two notifications on this composable
	 * @return a new {@link Stream}
	 * @since 1.1, 2.0
	 */
	public final Stream<O> timeout(long timeout) {
		return timeout(timeout, null);
	}

	/**
	 * Signal an error if no data has been emitted for {@param timeout} milliseconds. Timeout is run on the environment
	 * root timer. <p> A Timeout Exception will be signaled if no data or complete signal have been sent within the
	 * given period.
	 * @param timeout the timeout in unit between two notifications on this composable
	 * @param unit the time unit
	 * @return a new {@link Stream}
	 * @since 1.1, 2.0
	 */
	public final Stream<O> timeout(long timeout, TimeUnit unit) {
		return timeout(timeout, unit, null);
	}

	/**
	 * Switch to the fallback Publisher if no data has been emitted for {@param timeout} milliseconds. Timeout is run on
	 * the environment root timer. <p> The current subscription will be cancelled and the fallback publisher subscribed.
	 * <p> A Timeout Exception will be signaled if no data or complete signal have been sent within the given period.
	 * @param timeout the timeout in unit between two notifications on this composable
	 * @param unit the time unit
	 * @param fallback the fallback {@link Publisher} to subscribe to once the timeout has occured
	 * @return a new {@link Stream}
	 * @since 2.0
	 */
	public final Stream<O> timeout(final long timeout, final TimeUnit unit, final Publisher<? extends O> fallback) {
		final Timer timer = getTimer();
		Assert.state(timer != null, "Cannot use default timer as no environment has been provided to this " + "Stream");

		return lift(new TimeoutOperator<O>(fallback,
				timer,
				unit != null ? TimeUnit.MILLISECONDS.convert(timeout, unit) : timeout));
	}

	/**
	 * Print a debugged form of the root action relative to this one. The output will be an acyclic directed graph of
	 * composed actions.
	 *
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public ReactiveStateUtils.Graph debug() {
		return ReactiveStateUtils.scan(this);
	}

	/**
	 * Combine the most ancient upstream action to act as the {@link org.reactivestreams.Subscriber} downstream
	 * component and the current stream to act as the {@link org.reactivestreams.Publisher}. <p> Useful to share and
	 * ship a full stream whilst hiding the staging actions in the middle. <p> Default behavior, e.g. a single stream,
	 * will raise an {@link java.lang.IllegalStateException} as there would not be any Subscriber (Input) side to
	 * combine. {@link reactor.rx.stream.StreamOperator#combine()} is the usual reference implementation used.
	 * @param <E> the type of the most ancien action downstream.
	 * @return new Combined Action
	 */
	public <E> StreamProcessor<E, O> combine() {
		throw new IllegalStateException("Cannot combine a single Stream");
	}

	/**
	 * Return the promise of the next triggered signal. A promise is a container that will capture only once the first
	 * arriving error|next|complete signal to this {@link Stream}. It is useful to coordinate on single data streams or
	 * await for any signal.
	 * @return a new {@link Promise}
	 * @since 2.0
	 */
	public final Promise<O> next() {
		Promise<O> d = new Promise<O>(getTimer());
		subscribe(d);
		return d;
	}

	/**
	 * Return the promise of the next triggered signal. Unlike next, no extra action is required to request 1
	 * onSubscribe allowing for the promise request to run on the onSubscribe signal thread.
	 *
	 * A promise is a container that will capture only once the first arriving error|next|complete signal to this {@link
	 * Stream}. It is useful to coordinate on single data streams or await for any signal.
	 * @return a new {@link Promise}
	 * @since 2.1
	 */
	public final Promise<O> consumeNext() {
		Promise<O> d = new Promise<O>(getTimer());
		d.request(1);
		subscribe(d);
		return d;
	}

	/**
	 * Fetch all values in a List to the returned Promise
	 * @return the promise of all data from this Stream
	 * @since 2.0
	 */
	public final Promise<List<O>> toList() {
		return buffer(Integer.MAX_VALUE).next();
	}

	/**
	 * Fetch all values in a List to the returned Promise
	 * @return the promise of all data from this Stream
	 * @since 2.1
	 */
	public final Promise<List<O>> consumeAsList() {
		return buffer(Integer.MAX_VALUE).consumeNext();
	}

	/**
	 * Assign a Timer to be provided to this Stream Subscribers
	 * @param timer the timer
	 * @return a configured stream
	 */
	public Stream<O> timer(final Timer timer) {
		return new Lift<O, O>(this) {
			@Override
			public Timer getTimer() {
				return timer;
			}

			@Override
			public String getName() {
				return "timerSetup";
			}
		};

	}

	/**
	 * Blocking call to pass values from this stream to the queue that can be polled from a consumer.
	 * @return the buffered queue
	 * @since 2.0
	 */
	public final BlockingQueue<O> toBlockingQueue() {
		return toBlockingQueue(BaseProcessor.SMALL_BUFFER_SIZE);
	}

	/**
	 * Blocking call to eagerly fetch values from this stream
	 * @param maximum queue getCapacity(), a full queue might block the stream producer.
	 * @return the buffered queue
	 * @since 2.0
	 */
	@SuppressWarnings("unchecked")
	public final BlockingQueue<O> toBlockingQueue(int maximum) {
		return Publishers.toReadQueue(this, maximum);
	}

	@Override
	public long getCapacity() {
		return Long.MAX_VALUE;
	}

	/**
	 * Get the current timer available if any or try returning the shared Environment one (which may cause an error if
	 * no Environment has been globally initialized)
	 * @return any available timer
	 */
	public Timer getTimer() {
		return Timers.globalOrNull();
	}


	@Override
	public String toString() {
		return getClass().getSimpleName();
	}

	private final static Consumer NOOP = new Consumer() {
		@Override
		public void accept(Object o) {

		}
	};

	private static abstract class Lift<E, O> extends Stream<O> implements Upstream, Named {

		protected final Stream<E> origin;
		private final long capacity;

		public Lift(Stream<E> origin) {
			this.origin = origin;
			this.capacity = origin.getCapacity();
		}

		@Override
		@SuppressWarnings("unchecked")
		public void subscribe(Subscriber<? super O> s) {
			origin.subscribe((Subscriber<? super E>)s);
		}

		@Override
		public Object upstream() {
			return origin;
		}

		@Override
		public long getCapacity() {
			return capacity;
		}

		@Override
		public Timer getTimer() {
			return origin.getTimer();
		}

		@Override
		public String getName() {
			return "lift";
		}
	}

	private static final class DispatchOnLift<O> extends Lift<O, O> implements FeedbackLoop {

		private final ProcessorGroup processorProvider;

		public DispatchOnLift(Stream<O> s, ProcessorGroup processorProvider) {
			super(s);
			this.processorProvider = processorProvider;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void subscribe(Subscriber s) {
			try {
				Processor<O, O> processor = processorProvider.dispatchOn();
				processor.subscribe(s);
				origin.subscribe(processor);
			}
			catch (Throwable t) {
				s.onError(t);
			}
		}

		@Override
		public String getName() {
			return "dispatchOn";
		}

		@Override
		public Object delegateInput() {
			return processorProvider;
		}

		@Override
		public Object delegateOutput() {
			return processorProvider;
		}
	}

	private static final class PublishOnLift<O> extends Lift<O, O> implements ReactiveState.FeedbackLoop {

		private final ProcessorGroup processorProvider;

		public PublishOnLift(Stream<O> s, ProcessorGroup processorProvider) {
			super(s);
			this.processorProvider = processorProvider;
		}

		@Override
		@SuppressWarnings("unchecked")
		public void subscribe(Subscriber s) {
			try {
				Processor<O, O> processor = processorProvider.publishOn();
				processor.subscribe(s);
				origin.subscribe(processor);
			}
			catch (Throwable t) {
				s.onError(t);
			}
		}

		@Override
		public String getName() {
			return "publishOn";
		}

		@Override
		public Object delegateInput() {
			return processorProvider.delegateInput();
		}

		@Override
		public Object delegateOutput() {
			return processorProvider.delegateOutput();
		}
	}
}
