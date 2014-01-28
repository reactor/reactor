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

package reactor.core.composable;

import reactor.core.Environment;
import reactor.core.HashWheelTimer;
import reactor.core.Observable;
import reactor.core.action.*;
import reactor.core.composable.spec.DeferredStreamSpec;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.selector.Selector;
import reactor.function.*;
import reactor.function.support.Tap;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@code Stream} is a stateless event processor that provides methods for attaching {@link
 * Consumer Consumers} to consume the values passing through the stream. Some methods like
 * {@link #map(reactor.function.Function)}, {@link #filter(reactor.function.Predicate)},
 * {@link #first()}, {@link #last()}, and {@link #reduce(reactor.function.Function, Object)}
 * return new {@code Stream Streams} whose values are the result of transformation functions,
 * filtering, and the like.
 * <p/>
 * Typically, new {@code Stream Streams} aren't created directly. To create a {@code Stream},
 * create a {@link DeferredStreamSpec} and configure it with the appropriate {@link Environment},
 * {@link Dispatcher}, and other settings, then call {@link Deferred#compose()}, which will
 * return a {@code Stream} that handles the values passed into the {@link Deferred}.
 *
 * @param <T> the type of the values in the stream
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public class Stream<T> extends Composable<T> {

	protected final Environment environment;

  /**
	 * Create a new Stream that will use the {@link Observable} to pass its values to registered
	 * handlers.
   *
	 * Once all event handler have been registered, {@link #flush} must be called to pass the initial values
	 * to those handlers.
	 * The stream will accept errors from the given {@code parent}.
	 *
	 * @param observable     The observable used to drive event handlers
   * @param parent         The stream's parent. May be {@code null}
	 * @param acceptSelector The tuple Selector/Key to accept values on this observable. May be {@code null}
	 */
	public Stream(@Nullable final Observable observable,
                @Nullable final Composable<?> parent,
                Tuple2<Selector, Object> acceptSelector,
	              @Nullable final Environment environment) {
		super(observable, parent, acceptSelector);
    this.environment = environment;
	}

	@Override
	public Stream<T> consume(@Nonnull Consumer<T> consumer) {
		return (Stream<T>) super.consume(consumer);
	}

	@Override
	public Stream<T> connect(@Nonnull Composable<T> consumer) {
		return (Stream<T>) super.connect(consumer);
	}

	@Override
	public Stream<T> consume(@Nonnull Object key, @Nonnull Observable observable) {
		return (Stream<T>) super.consume(key, observable);
	}

	@Override
	public Stream<T> flush() {
		return (Stream<T>) super.flush();
	}

  @Override
	public <E extends Throwable> Stream<T> when(@Nonnull Class<E> exceptionType, @Nonnull Consumer<E> onError) {
		return (Stream<T>) super.when(exceptionType, onError);
	}

	@Override
	public <V> Stream<V> map(@Nonnull Function<T, V> fn) {
		return (Stream<V>) super.map(fn);
	}

	@Override
	public <V, C extends Composable<V>> Stream<V> mapMany(@Nonnull Function<T, C> fn) {
		return (Stream<V>) super.mapMany(fn);
	}


	@Override
	public Stream<T> filter(@Nonnull Predicate<T> p) {
		return (Stream<T>) super.filter(p);
	}

	@Override
	public Stream<T> filter(@Nonnull Predicate<T> p, Composable<T> composable) {
		return (Stream<T>) super.filter(p, composable);
	}

	/**
	 * Create a new {@code Stream} whose values will be only the first value of each batch. Requires a {@code batchSize}
	 * to
	 * have been set.
	 * <p/>
	 * When a new batch is triggered, the first value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @return a new {@code Stream} whose values are the first value of each batch
	 */
	public Stream<T> first() {
    final Deferred<T, Stream<T>> d = createDeferredChildStream(0);

    final AtomicReference<Event<T>> ref = new AtomicReference();
    add(new Action<T>(getObservable(), getError().getObject(), d.compose().getAcceptKey()) {
      @Override
      protected void doAccept(Event<T> ev) {
        if (null == ref.get()) {
          ref.set(ev);
          getObservable().notify(d.compose().getAcceptKey(), ev);
        }
      }
    });

    return d.compose();
  }

	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code batchSize}
	 *
	 * @return a new {@code Stream} whose values are the last value of each batch
	 */
	public Stream<T> last() {
    final Deferred<T, Stream<T>> d = createDeferredChildStream(0);

    final AtomicReference<Event<T>> ref = new AtomicReference();
    add(new Action<T>(getObservable(), getError().getObject(), d.compose().getAcceptKey()) {
      @Override
      protected void doAccept(Event<T> ev) {
        ref.set(ev);
        getObservable().notify(d.compose().getAcceptKey(), ev);
      }
    });

    return d.compose();
  }

  /**
   * Create a new {@code Stream} whose values will be each element E of any Iterable<E> flowing this Stream
   * <p/>
   * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
   *
   * @return a new {@code Stream} whose values result from the iterable input
   */
  public Stream<T> split() {
    final Deferred<T, Stream<T>> d = createDeferred();
    getObservable().on(getAcceptSelector(),
                       new ForEachAction<T>(0,
                                            getObservable(),
                                            d.compose().getAcceptKey(),
                                            getError().getObject()));
    return d.compose();
  }

	/**
	 * Indicates whether or not this {@code Stream} is unbounded.
	 *
	 * @return {@literal true} if a {@code batchSize} has been set, {@literal false} otherwise
	 */
	public boolean isBatch() {
		return false;
	}

	/**
	 * Create a {@link Tap} that maintains a reference to the last value seen by this {@code Stream}. The {@link Tap} is
	 * continually updated when new values pass through the {@code Stream}.
	 *
	 * @return the new {@link Tap}
	 * @see Supplier
	 */
	public Tap<T> tap() {
		final Tap<T> tap = new Tap<T>();
		consume(tap);
		return tap;
	}


	/**
	 * Use {@link reactor.core.Observable#batchNotify(Object)} to group dispatching by the current batch size.
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @return a new {@code Stream} whose values result from the iterable input
	 */
	public Stream<T> buffer() {
		return buffer(0);
	}

	/**
	 * Use {@link reactor.core.Observable#batchNotify(Object)} to group dispatch by the passed {@param
	 * batchSize}.
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @param batchSize the batch size to use
	 * @return a new {@code Stream} whose values result from the iterable input
	 */
	public Stream<T> buffer(int batchSize) {
		Assert.state(batchSize > 0, "Cannot batch() an unbounded Stream. Try setting a batchSize greater than 0.");
		final Deferred<T, Stream<T>> d = createDeferred(batchSize);
		add(new BufferAction<T>(batchSize,
				d.compose().getObservable(),
				d.compose().getAcceptKey(),
				getError().getObject()));
		return d.compose();
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
	 * batchSize} has been reached.
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this batch
	 */
	public Stream<Iterable<T>> collect() {
    throw new RuntimeException("Can't collect on unbounded stream");
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
	 * batchSize} has been reached.
	 *
	 * @param batchSize the collected size
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this batch
	 */
	public Stream<Iterable<T>> collect(int batchSize) {
		Assert.state(batchSize > 0, "Cannot collect() an unbounded limit.");
		final Deferred<Iterable<T>, Stream<Iterable<T>>> d = createDeferredIterableChildStream(1);

		add(new CollectAction<T>(
				batchSize,
				d.compose().getObservable(),
				d.compose().getAcceptKey(),
				getError().getObject()));

		return d.compose();
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every specified
	 * time from the {@param period} in milliseconds. The window runs on a timer from the stream {@link
	 * this#environment}.
	 *
	 * @param period the time period when each window close and flush the attached consumer
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<Iterable<T>> window(int period) {
		return window(period, TimeUnit.MILLISECONDS);
	}

	/**
	 * Collect incoming values into an internal array, providing a {@link List} that will be pushed into the returned
	 * {@code Stream} every specified {@param period} in milliseconds. The window runs on a timer from the stream {@link
	 * this#environment}. After accepting {@param backlog} of items, every old item will be dropped. Resulting {@link
	 * List} will be at most {@param backlog} items long.
	 *
	 * @param period  the time period when each window close and flush the attached consumer
	 * @param backlog maximum amount of items to keep
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<Iterable<T>> movingWindow(int period, int backlog) {
		return movingWindow(period, TimeUnit.MILLISECONDS, backlog);
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every specified
	 * time from the {@param period} and a {@param timeUnit}. The window
	 * runs on a timer from the stream {@link this#environment}.
	 *
	 * @param period   the time period when each window close and flush the attached consumer
	 * @param timeUnit the time unit used for the period
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<Iterable<T>> window(int period, TimeUnit timeUnit) {
		return window(period, timeUnit, 0);
	}

	/**
	 * Collect incoming values into an internal array, providing a {@link List} that will be pushed into the returned
	 * {@code Stream} every specified time from the {@param period} and a {@param timeUnit}.
	 *
	 * @param period   the time period when each window close and flush the attached consumer
	 * @param timeUnit the time unit used for the period
	 * @param backlog  maximum amount of items to keep
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<Iterable<T>> movingWindow(int period, TimeUnit timeUnit, int backlog) {
		return movingWindow(period, timeUnit, 0, backlog);
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every specified
	 * time from the {@param period}, {@param timeUnit} after an initial {@param delay} in milliseconds. The window
	 * runs on a timer from the stream {@link this#environment}.
	 *
	 * @param period   the time period when each window close and flush the attached consumer
	 * @param timeUnit the time unit used for the period
	 * @param delay    the initial delay in milliseconds
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<Iterable<T>> window(int period, TimeUnit timeUnit, int delay) {
		Assert.state(environment != null, "Cannot use default timer as no environment has been provided to this Stream");
		return window(period, timeUnit, delay, environment.getRootTimer());
	}

	/**
	 * Collect incoming values into an internal array, providing a {@link List} that will be pushed into the returned
	 * {@code Stream} every specified time from the {@param period} and a {@param timeUnit} after an initial {@param delay}
	 * in milliseconds.
	 *
	 * @param period   the time period when each window close and flush the attached consumer
	 * @param timeUnit the time unit used for the period
	 * @param delay    the initial delay in milliseconds
	 * @param backlog  maximum amount of items to keep
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<Iterable<T>> movingWindow(int period, TimeUnit timeUnit, int delay, int backlog) {
		Assert.state(environment != null, "Cannot use default timer as no environment has been provided to this Stream");
		return movingWindow(period, timeUnit, delay, backlog, environment.getRootTimer());
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every specified
	 * time from the {@param period}, {@param timeUnit} after an initial {@param delay} in milliseconds. The window
	 * runs on a supplied {@param timer}.
	 *
	 * @param period   the time period when each window close and flush the attached consumer
	 * @param timeUnit the time unit used for the period
	 * @param delay    the initial delay in milliseconds
	 * @param timer    the reactor timer to run the window on
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<Iterable<T>> window(int period, TimeUnit timeUnit, int delay, HashWheelTimer timer) {
		Assert.state(timer != null, "Timer must be supplied");
		final Deferred<Iterable<T>, Stream<Iterable<T>>> d = createDeferredIterableChildStream(1);

		add(new WindowAction<T>(
				d.compose().getObservable(),
				d.compose().getAcceptKey(),
				getError().getObject(),
				timer,
				period, timeUnit, delay
		));

		return d.compose();
	}

	/**
	 * Collect incoming values into an internal array, providing a {@link List} that will be pushed into the returned
	 * {@code Stream} every specified time from the {@param period} and a {@param timeUnit} after an initial {@param delay}
	 * in milliseconds.
	 *
	 * @param period   the time period when each window close and flush the attached consumer
	 * @param timeUnit the time unit used for the period
	 * @param delay    the initial delay in milliseconds
	 * @param backlog  maximum amount of items to keep
	 * @param timer    the reactor timer to run the window on
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this window
	 */
	public Stream<Iterable<T>> movingWindow(int period, TimeUnit timeUnit, int delay, int backlog, HashWheelTimer timer) {
		Assert.state(timer != null, "Timer must be supplied");
		final Deferred<Iterable<T>, Stream<Iterable<T>>> d = createDeferredIterableChildStream(0);

		add(new MovingWindowAction<T>(d.compose().getObservable(),
                                  d.compose().getAcceptKey(),
                                  getError().getObject(),
                                  timer,
                                  period,
                                  timeUnit,
                                  delay,
                                  backlog));

		return d.compose();
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}. The given initial object will be
	 * passed to the function's {@link Tuple2} argument.
	 *
	 * @param fn      the reduce function
	 * @param initial the initial argument to pass to the reduce function
	 * @param <A>     the type of the reduced object
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Stream<A> reduce(@Nonnull Function<Tuple2<T, A>, A> fn, A initial) {
		return reduce(fn, Functions.supplier(initial));
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}. The given {@link Supplier} will be
	 * used to produce initial accumulator objects either on the first reduce call, in the case of an unbounded {@code
	 * Stream}, or on the first value of each batch, if a {@code batchSize} is set.
	 * <p/>
	 * In an unbounded {@code Stream}, the accumulated value will be published on the returned {@code Stream} every time
	 * a
	 * value is accepted. But when a {@code batchSize} has been set and {@link #isBatch()} returns true, the accumulated
	 * value will only be published on the new {@code Stream} at the end of each batch. On the next value (the first of
	 * the next batch), the {@link Supplier} is called again for a new accumulator object and the reduce starts over with
	 * a new accumulator.
	 *
	 * @param fn           the reduce function
	 * @param accumulators the {@link Supplier} that will provide accumulators
	 * @param <A>          the type of the reduced object
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Stream<A> reduce(@Nonnull final Function<Tuple2<T, A>, A> fn, @Nullable final Supplier<A> accumulators) {
		final Deferred<A, Stream<A>> d = createDeferred();
		final Stream<A> stream = d.compose();

    add(new ScanAction<T, A>(accumulators,
                             fn,
                             stream.getObservable(),
                             stream.getAcceptKey(),
                             getError().getObject()));

		return d.compose();
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}.
	 *
	 * @param fn  the reduce function
	 * @param <A> the type of the reduced object
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Stream<A> reduce(@Nonnull final Function<Tuple2<T, A>, A> fn) {
		return reduce(fn, (Supplier<A>) null);
	}


  @SuppressWarnings("unchecked")
  protected <V, C extends Composable<V>> Deferred<V, C> createDeferred() {
    return createDeferred(0);
  }

  @SuppressWarnings("unchecked")
  protected <V, C extends Composable<V>> Deferred<V, C> createDeferred(int batchSize) {
    return (Deferred<V, C>) createDeferredChildStream(batchSize);
  }

  private Deferred<T, Stream<T>> createDeferredChildStream(int batchSize) {
    Stream<T> stream;
    if (batchSize > 1) {
      stream = new BufferStream<T>(null,
                                   batchSize,
                                   null,
                                   this,
                                   null,
                                   environment);
    } else {
      stream = new Stream<T>(null,
                             this,
                             null,
                             environment);
    }
    return new Deferred<T, Stream<T>>(stream);
  }

  private Deferred<Iterable<T>, Stream<Iterable<T>>> createDeferredIterableChildStream(int batchSize) {
    Stream<Iterable<T>> stream = new BufferStream<Iterable<T>>(null,
                                                               batchSize,
                                                               null,
                                                               this,
                                                               null,
                                                               environment);
    return new Deferred<Iterable<T>, Stream<Iterable<T>>>(stream);
  }



}