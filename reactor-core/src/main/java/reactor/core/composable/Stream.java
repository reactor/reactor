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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import reactor.core.Environment;
import reactor.core.Observable;
import reactor.core.composable.spec.DeferredStreamSpec;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.registry.Registration;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.event.support.CallbackEvent;
import reactor.event.support.EventConsumer;
import reactor.function.*;
import reactor.function.support.Tap;
import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

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
 * @param <T>
 * 		the type of the values in the stream
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public class Stream<T> extends Composable<T> {

	private final Tuple2<Selector, Object> first = Selectors.$();
	private final Tuple2<Selector, Object> last  = Selectors.$();
	private final int                              batchSize;
	private final Iterable<T>                      values;

	/**
	 * Create a new Stream that will use the {@link Dispatcher} to pass its values to registered
	 * handlers.
	 * </p>
	 * The stream will batch values into batches of the given
	 * {@code batchSize}, affecting the values that are passed to the {@link #first()} and {@link
	 * #last()} substreams. A size of {@code -1} indicates that the stream should not be batched.
	 * </p>
	 * The stream will be pre-populated with the given initial {@code values}. Once all event
	 * handler have been registered, {@link #flush} must be called to pass the initial values
	 * to those handlers.
	 * </p>
	 * The stream will accept errors from the given {@code parent}.
	 *
	 * @param dispatcher
	 * 		The dispatcher used to drive event handlers
	 * @param batchSize
	 * 		The size of the batches, or {@code -1} for no batching
	 * @param values
	 * 		The stream's initial values. May be {@code null}
	 * @param parent
	 * 		The stream's parent. May be {@code null}
	 */
	public Stream(@Nonnull Dispatcher dispatcher,
	              int batchSize,
	              @Nullable Iterable<T> values,
	              @Nullable Composable<?> parent) {
		super(dispatcher, parent);
		this.batchSize = batchSize;
		this.values = values;

		getObservable().on(getFlush().getT1(), new Consumer<Event<Void>>() {
			@Override
			public void accept(Event<Void> ev) {
				if (null == Stream.this.values) {
					return;
				}
				for (T val : Stream.this.values) {
					Stream.this.notifyValue(val);
				}
			}
		});
	}

	@Override
	public Stream<T> consume(@Nonnull Consumer<T> consumer) {
		return (Stream<T>) super.consume(consumer);
	}

	@Override
	public Stream<T> consume(@Nonnull Composable<T> consumer) {
		return (Stream<T>) super.consume(consumer);
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
	 *
	 * @see #batch(int)
	 */
	public Stream<T> first() {
		Deferred<T, Stream<T>> d = createDeferredChildStream();
		getObservable().on(first.getT1(), new EventConsumer<T>(d));
		return d.compose();
	}

	/**
	 * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code batchSize} to
	 * have been set.
	 * <p/>
	 * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
	 *
	 * @return a new {@code Stream} whose values are the last value of each batch
	 */
	public Stream<T> last() {
		Deferred<T, Stream<T>> d = createDeferredChildStream();
		getObservable().on(last.getT1(), new EventConsumer<T>(d));
		return d.compose();
	}

	/**
	 * Create a new {@code Stream} with the given {@code batchSize}. When a {@code batchSize} has been set, {@link
	 * #first()} and {@link #last()} and {@link #collect()} are available, since a fixed number of values is expected. In
	 * an unbounded {@code Stream}, those methods have no meaning because there is no beginning or end of an unbounded
	 * {@code Stream}.
	 *
	 * @param batchSize
	 * 		the batch size to use
	 *
	 * @return a new {@code Stream} containing the values from the parent
	 */
	public Stream<T> batch(final int batchSize) {
		final Deferred<T, Stream<T>> d = createDeferred(batchSize);
		consume(d);
		return d.compose();
	}

	/**
	 * Indicates whether or not this {@code Stream} is unbounded.
	 *
	 * @return {@literal true} if a {@code batchSize} has been set, {@literal false} otherwise
	 */
	public boolean isBatch() {
		return batchSize > 0;
	}

	/**
	 * Create a {@link Tap} that maintains a reference to the last value seen by this {@code Stream}. The {@link Tap} is
	 * continually updated when new values pass through the {@code Stream}.
	 *
	 * @return the new {@link Tap}
	 *
	 * @see Supplier
	 */
	public Tap<T> tap() {
		final Tap<T> tap = new Tap<T>();
		consume(tap);
		return tap;
	}

	/**
	 * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
	 * batchSize} has been reached.
	 *
	 * @return a new {@code Stream} whose values are a {@link List} of all values in this batch
	 */
	public Stream<List<T>> collect() {
		Assert.state(batchSize > 0, "Cannot collect() an unbounded Stream. Try extracting a batch first.");
		final Deferred<List<T>, Stream<List<T>>> d = createDeferred(batchSize);
		final List<T> values = new ArrayList<T>();

		consumeEvent(new Consumer<Event<T>>() {
			@Override
			public void accept(Event<T> value) {
				synchronized(values) {
					values.add(value.getData());
					if(values.size() % batchSize != 0) {
						return;
					}
					d.acceptEvent(value.copy((List<T>)new ArrayList<T>(values)));
					values.clear();
				}
			}
		});

		getObservable().on(getFlush().getT1(), new Consumer<Event<Void>>() {
			@Override public void accept(Event<Void> ev) {
				synchronized(values) {
					if(values.isEmpty()) {
						return;
					}
					d.accept(new ArrayList<T>(values));
					values.clear();
				}
			}
		});

		return d.compose();
	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}. The given initial object will be
	 * passed to the function's {@link Tuple2} argument.
	 *
	 * @param fn
	 * 		the reduce function
	 * @param initial
	 * 		the initial argument to pass to the reduce function
	 * @param <A>
	 * 		the type of the reduced object
	 *
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
	 * In an unbounded {@code Stream}, the accumulated value will be published on the returned {@code Stream} every time a
	 * value is accepted. But when a {@code batchSize} has been set and {@link #isBatch()} returns true, the accumulated
	 * value will only be published on the new {@code Stream} at the end of each batch. On the next value (the first of
	 * the next batch), the {@link Supplier} is called again for a new accumulator object and the reduce starts over with
	 * a new accumulator.
	 *
	 * @param fn
	 * 		the reduce function
	 * @param accumulators
	 * 		the {@link Supplier} that will provide accumulators
	 * @param <A>
	 * 		the type of the reduced object
	 *
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Stream<A> reduce(@Nonnull final Function<Tuple2<T, A>, A> fn, @Nullable final Supplier<A> accumulators) {
		final Deferred<A, Stream<A>> d = createDeferred();

    ReduceConsumer reduceConsumer = new ReduceConsumer(d, fn, accumulators, this.batchSize);
    this.consumeEvent(reduceConsumer);
    return reduceConsumer.getChildStream();

	}

	/**
	 * Reduce the values passing through this {@code Stream} into an object {@code A}.
	 *
	 * @param fn
	 * 		the reduce function
	 * @param <A>
	 * 		the type of the reduced object
	 *
	 * @return a new {@code Stream} whose values contain only the reduced objects
	 */
	public <A> Stream<A> reduce(@Nonnull final Function<Tuple2<T, A>, A> fn) {
		return reduce(fn, (Supplier<A>)null);
	}

	@Override
	protected <V, C extends Composable<V>> Deferred<V, C> createDeferred() {
		return createDeferred(batchSize);
	}

	@SuppressWarnings("unchecked")
	protected <V, C extends Composable<V>> Deferred<V, C> createDeferred(int batchSize) {
		return (Deferred<V, C>)createDeferredChildStream(batchSize);
	}

	private Deferred<T, Stream<T>> createDeferredChildStream() {
		return createDeferredChildStream(-1);
	}

	private Deferred<T, Stream<T>> createDeferredChildStream(int batchSize) {
    return new Deferred<T, Stream<T>>(new Stream<T>(new SynchronousDispatcher(),
		                                                batchSize,
		                                                null,
		                                                this));
	}

	@Override
	protected void errorAccepted(Throwable error) {
	}

	@Override
	protected void valueAccepted(final T value) {
		if(!isBatch()) {
			return;
		}
		long accepted = getAcceptCount() % batchSize;
		if(accepted == 1) {
			getObservable().notify(first.getT2(), Event.wrap(value));
		} else if(accepted == 0) {
			getObservable().notify(last.getT2(), Event.wrap(value));
		}
	}

	@Override
	public String toString() {
		return "Stream{" +
				"acceptCount=" + getAcceptCount() +
				", errorCount=" + getErrorCount() +
				", batchSize=" + batchSize +
				", values=" + values +
				'}';
	}

}