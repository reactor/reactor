/*
 * Copyright (c) 2011-2013 the original author or authors.
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

package reactor.core;

import org.slf4j.LoggerFactory;
import reactor.Fn;
import reactor.fn.*;
import reactor.fn.dispatch.Dispatcher;
import reactor.fn.dispatch.SynchronousDispatcher;
import reactor.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static reactor.Fn.$;

/**
 * A {@literal Composable} is a way to provide components when other threads to act on incoming data and provide new
 * data to other components that must wait on the data to become available.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public class Composable<T> implements Consumer<T>, Supplier<T>, Deferred<T> {

	private static final String   EXPECTED_ACCEPT_LENGTH_HEADER = "x-reactor-expectedAcceptCount";
	private static       long     DEFAULT_TIMEOUT               = 30l;
	private static       TimeUnit DEFAULT_TIMEUNIT              = TimeUnit.SECONDS;

	protected static final SynchronousDispatcher SYNCHRONOUS_DISPATCHER = new SynchronousDispatcher();

	static {
		String s = System.getProperty("reactor.max.await.timeout");
		if (null != s && s.length() > 0) {
			TimeUnit unit = TimeUnit.SECONDS;
			if (s.endsWith("ns")) {
				s = s.substring(0, s.length() - 2);
				unit = TimeUnit.NANOSECONDS;
			} else if (s.endsWith("ms")) {
				s = s.substring(0, s.length() - 2);
				unit = TimeUnit.MILLISECONDS;
			} else if (s.endsWith("s")) {
				s = s.substring(0, s.length() - 1);
				unit = TimeUnit.SECONDS;
			}
			try {
				long l = Long.parseLong(s);
				DEFAULT_TIMEOUT = l;
				DEFAULT_TIMEUNIT = unit;
			} catch (NumberFormatException ignored) {
				LoggerFactory.getLogger(Composable.class).error(ignored.getMessage(), ignored);
			}
		}
	}

	protected final Object monitor = new Object();

	protected final Object   acceptKey      = new Object();
	protected final Selector acceptSelector = $(acceptKey);

	protected final Object   firstKey      = new Object();
	protected final Selector firstSelector = $(firstKey);

	protected final Object   lastKey      = new Object();
	protected final Selector lastSelector = $(lastKey);

	protected final AtomicLong acceptedCount       = new AtomicLong(0);
	protected final AtomicLong expectedAcceptCount = new AtomicLong(-1);


	protected final Observable observable;
	protected boolean hasBlockers = false;
	protected T         value;
	protected Throwable error;

	/**
	 * Create a {@literal Composable} with default behavior.
	 */
	public Composable() {
		this.observable = createObservable((Observable) null);
	}

	public Composable(Dispatcher dispatcher) {
		this.observable = createObservable(dispatcher);
	}

	/**
	 * Create a {@literal Composable}.
	 *
	 * @param <T> The type of the value.
	 * @return The new {@literal Promise}.
	 */
	public static <T> Composable<T> create() {
		return new Composable<T>();
	}

	/**
	 * Create a {@link Composable} from the given {@link Composable#observable} and consume it.
	 *
	 * @param src The composable to defer.
	 * @param <T> The type of the values.
	 * @return a new {@link Composable}
	 */
	public static <T> Composable<T> from(Composable<T> src) {
		final Composable<T> c = new Composable<T>(src);
		src.consume(new Consumer<T>() {
			@Override
			public void accept(T t) {
				c.accept(t);
			}
		});
		return c;
	}


	/**
	 * Create a {@literal Composable} when the given {@code key} and {@link Event} and delay notification of the event on
	 * the given {@link Observable} until the returned {@link Composable}'s {@link Composable#await(long,
	 * java.util.concurrent.TimeUnit)} or {@link Composable#get()} methods are called.
	 *
	 * @param key        The key to use when notifying the {@link Observable}.
	 * @param observable The {@link Observable} on which to invoke the notify method.
	 * @param <T>        The type of the {@link Event} data.
	 * @return The new {@literal Composable}.
	 */
	public static <T, E extends Event<T>> Composable<E> to(final Object key, final Observable observable) {
		Assert.notNull(observable);
		return new Builder<E>()
				.get()
				.consume(new Consumer<E>() {
					@Override
					public void accept(E e) {
						observable.notify(key, e, null);
					}
				});
	}

	/**
	 * Create a {@link Composable} that uses the given {@link Reactor} for publishing events internally.
	 *
	 * @param observable The {@link Reactor} to use.
	 */
	public Composable(Observable observable) {
		this.observable = observable;
	}


	/**
	 * Create a {@link Composable} that uses the given {@link Composable} {@link Observable} for publishing events internally.
	 *
	 * @param composable The {@link Composable} to use.
	 */
	public Composable(Composable composable) {
		this.observable = composable.observable;
	}

	/**
	 * Set the number of times to expect {@link #accept(Object)} to be called.
	 *
	 * @param expectedAcceptCount The number of times {@link #accept(Object)} will be called.
	 * @return {@literal this}
	 */
	public Composable<T> setExpectedAcceptCount(long expectedAcceptCount) {
		this.expectedAcceptCount.set(expectedAcceptCount);
		if (this.acceptedCount.get() >= expectedAcceptCount) {
			observable.notify(lastKey, Fn.event(value));
			synchronized (monitor) {
				monitor.notifyAll();
			}
		}
		return this;
	}

	/**
	 * Register a {@link Consumer} that will be invoked whenever {@link #accept(Object)} is called.
	 *
	 * @param consumer The consumer to invoke.
	 * @return {@literal this}
	 * @see {@link #accept(Object)}
	 */
	public Composable<T> consume(Consumer<T> consumer) {
		when(acceptSelector, consumer);
		return this;
	}

	/**
	 * Register a {@code key} and {@link Reactor} on which to publish an event whenever {@link #accept(Object)} is called.
	 *
	 * @param key        The key to use when publishing the {@link Event}.
	 * @param observable The {@link Observable} on which to publish the {@link Event}.
	 * @return {@literal this}
	 */
	public Composable<T> consume(final Object key, final Observable observable) {
		Assert.notNull(observable);
		when(acceptSelector, new Consumer<T>() {
			@Override
			public void accept(T event) {
				observable.notify(key, Event.class.isAssignableFrom(event.getClass()) ? (Event<?>) event : Fn.event(event));
			}
		});
		return this;
	}

	/**
	 * Creates a new {@link Composable} that will be triggered once, the first time {@link #accept(Object)} is called on
	 * the parent.
	 *
	 * @return A new {@link Composable} that is linked to the parent.
	 */
	public Composable<T> first() {
		final Composable<T> c = createComposable(observable);
		c.expectedAcceptCount.set(1);
		when(firstSelector, new Consumer<T>() {
			@Override
			public void accept(T t) {
				c.accept(t);
			}
		});
		return c;
	}

	/**
	 * Creates a new {@link Composable} that will be triggered once, the last time {@link #accept(Object)} is called on the
	 * parent.
	 *
	 * @return A new {@link Composable} that is linked to the parent.
	 * @see {@link #setExpectedAcceptCount(long)}
	 */
	public Composable<T> last() {
		final Composable<T> c = createComposable(observable);
		c.expectedAcceptCount.set(1);
		when(lastSelector, new Consumer<T>() {
			@Override
			public void accept(T t) {
				c.accept(t);
			}
		});
		return c;
	}


	/**
	 * Create a new {@link Composable} that is linked to the parent through the given {@link Function}. When the parent's
	 * {@link #accept(Object)} is invoked, this {@link Function} is invoked and the result is passed into the returned
	 * {@link Composable}.
	 *
	 * @param fn  The transformation function to apply.
	 * @param <V> The type of the object returned when the given {@link Function}.
	 * @return The new {@link Composable}.
	 */
	public <V> Composable<V> map(final Function<T, V> fn) {
		Assert.notNull(fn);
		final Composable<V> c = createComposable(createObservable(observable));
		when(acceptSelector, new Consumer<T>() {
			@Override
			public void accept(T value) {
				try {
					c.accept(fn.apply(value));
				} catch (Throwable t) {
					handleError(c, t);
				}
			}
		});
		return c;
	}

	/**
	 * Create a new {@link Composable} that is linked to the parent through the given {@code key} and {@link Observable}.
	 * When the parent's {@link #accept(Object)} is invoked, its value is wrapped into an {@link Event} and passed to
	 * {@link Observable#notify (reactor.fn.Event)} along with the given {@code key}. After the event is being propagated
	 * to the reactor consumers, the new composition expects {@param <V>} replies to be returned.
	 *
	 * @param key        The key to notify
	 * @param observable The observable to notify
	 * @param <V>        The type of the object returned by reactor reply.
	 * @return The new {@link Composable}.
	 */
	public <V> Composable<V> map(final Object key, final Observable observable) {
		Assert.notNull(observable);
		final Composable<V> c = createComposable(createObservable(observable));
		final Object replyTo = new Object();

		observable.on($(replyTo), new Consumer<Event<V>>() {
			@Override
			public void accept(Event<V> event) {
				try {
					c.accept(event.getData());
				} catch (Throwable t) {
					handleError(c, t);
				}
			}
		});

		when(acceptSelector, new Consumer<T>() {
			@Override
			public void accept(T value) {
				try {
					Event<?> event = Event.class.isAssignableFrom(value.getClass()) ? (Event<?>) value : Fn.event(value);
					event.setReplyTo(replyTo);
					//event.getHeaders().setOrigin(reactor.getId());
					observable.send(key, event);
				} catch (Throwable t) {
					handleError(c, t);
				}
			}
		});
		return c;
	}

	/**
	 * Accumulate a result until expected accept count has been reached - If this limit hasn't been set, each accumulated
	 * result will notify the returned {@link Composable}. A {@link Function} taking a {@link Reduce} argument must be
	 * passed to process each pair formed of the last accumulated result and a new value to be processed.
	 *
	 * @param fn      The reduce function
	 * @param initial The initial accumulated result value e.g. an empty list.
	 * @param <V>     The type of the object returned by reactor reply.
	 * @return The new {@link Composable}.
	 */
	public <V> Composable<V> reduce(final Function<Reduce<T, V>, V> fn, V initial) {
		Assert.notNull(fn);
		final AtomicReference<V> lastValue = new AtomicReference<V>(initial);
		final Composable<V> c = createComposable(createObservable(observable));
		c.setExpectedAcceptCount(1);
		when(lastSelector, new Consumer<T>() {
			@Override
			public void accept(T t) {
				c.accept(lastValue.get());
			}
		});
		when(acceptSelector, new Consumer<T>() {
			@Override
			public void accept(T value) {
				try {
					Reduce<T, V> r = new Reduce<T, V>(lastValue.get(), value);
					lastValue.set(fn.apply(r));
					if (expectedAcceptCount.get() < 0) {
						c.accept(lastValue.get());
					}
				} catch (Throwable t) {
					handleError(c, t);
				}
			}
		});
		return c;
	}


	/**
	 * Accumulate a result until expected accept count has been reached - If this limit hasn't been set, each accumulated
	 * result will notify the returned {@link Composable}. Will automatically generate a collection formed from
	 * composable
	 * streamed results, until accept count is reached.
	 *
	 * @return The new {@link Composable}.
	 */
	public Composable<Collection<T>> reduce() {
		return reduce(new Function<Reduce<T, Collection<T>>, Collection<T>>() {
			@Override
			public Collection<T> apply(Reduce<T, Collection<T>> reducer) {
				reducer.getLastValue().add(reducer.getNextValue());
				return reducer.getLastValue();
			}
		}, new ArrayList<T>());
	}

	/**
	 * Take {@param count} number of values and send lastSelector event after {@param count} iterations
	 *
	 * @param count Number of values to accept
	 * @return The new {@link Composable}.
	 */
	public Composable<T> take(final long count) {
		final AtomicLong cursor = new AtomicLong(count);
		final AtomicReference<Registration<Consumer<Event<T>>>> reg = new
				AtomicReference<Registration<Consumer<Event<T>>>>();
		final Composable<T> c = createComposable(createObservable(observable));
		c.setExpectedAcceptCount(count);
		reg.set(when(acceptSelector, new Consumer<T>() {
			@Override
			public void accept(T value) {
				try {
					long _cursor = cursor.decrementAndGet();
					if (_cursor == 0) {
						reg.get().cancel();
					}
					if (_cursor >= 0) {
						c.accept(value);
					}
				} catch (Throwable t) {
					handleError(c, t);
				}
			}
		}));
		return c;
	}

	/**
	 * Accumulate a result until expected accept count has been reached - If this limit hasn't been set, each accumulated
	 * result will notify the returned {@link Composable}. A {@link Function} taking a {@link Reduce} argument must be
	 * passed to process each pair formed of the last accumulated result and a new value to be processed.
	 *
	 * @param fn  The reduce function
	 * @param <V> The type of the object returned by reactor reply.
	 * @return The new {@link Composable}.
	 */
	public <V> Composable<V> reduce(final Function<Reduce<T, V>, V> fn) {
		return reduce(fn, null);
	}


	/**
	 * Selectively call the returned {@link Composable} depending on the predicate {@link Function} argument
	 *
	 * @param fn The filter function, taking argument {@param <T>} and returning a {@link Boolean}
	 * @return The new {@link Composable}.
	 */
	public Composable<T> filter(final Function<T, Boolean> fn) {
		Assert.notNull(fn);
		final Composable<T> c = createComposable(createObservable(observable));
		when(acceptSelector, new Consumer<T>() {
			@Override
			public void accept(T value) {
				try {
					if (fn.apply(value)) {
						c.accept(value);
					} else {
						c.decreaseAcceptLength();
					}
				} catch (Throwable t) {
					handleError(c, t);
				}
			}
		});
		return c;
	}


	/**
	 * Trigger composition with an exception to be processed by dedicated consumers
	 *
	 * @param error The exception
	 */
	public void accept(Throwable error) {
		synchronized (monitor) {
			this.error = error;
			if (hasBlockers) {
				monitor.notifyAll();
			}
		}
		observable.notify(error.getClass(), Fn.event(error));
	}

	/**
	 * Trigger composition with a value to be processed by dedicated consumers
	 *
	 * @param value The exception
	 */
	public void accept(T value) {
		synchronized (monitor) {
			this.value = value;
			if (hasBlockers) {
				monitor.notifyAll();
			}
		}
		acceptedCount.incrementAndGet();
		observable.notify(acceptKey, Fn.event(value));
	}

	@Override
	public T await() throws InterruptedException {
		return await(DEFAULT_TIMEOUT, DEFAULT_TIMEUNIT);
	}

	@Override
	public T await(long timeout, TimeUnit unit) throws InterruptedException {
		synchronized (monitor) {
			if (isComplete()) {
				return get();
			}
			if (timeout >= 0) {
				hasBlockers = true;
				long msTimeout = TimeUnit.MILLISECONDS.convert(timeout, unit);
				long endTime = System.currentTimeMillis() + msTimeout;
				long now;
				while (!isComplete() && (now = System.currentTimeMillis()) < endTime) {
					this.monitor.wait(endTime - now);
				}
			} else {
				while (!isComplete()) {
					this.monitor.wait();
				}
			}
			hasBlockers = false;
		}
		return get();
	}

	protected boolean isComplete() {
		return isError() || acceptCountReached();
	}

	protected boolean isError() {
		synchronized (monitor) {
			return null != error;
		}
	}

	protected boolean acceptCountReached() {
		long expectedAcceptCount = this.expectedAcceptCount.get();
		return expectedAcceptCount >= 0 && acceptedCount.get() >= expectedAcceptCount;
	}

	@Override
	public T get() {
		synchronized (this.monitor) {
			if (null != error) {
				throw new IllegalStateException(error);
			}
			return value;
		}
	}

	/**
	 * Register a {@link Consumer} to be invoked whenever an exception that is assignable when the given exception type.
	 *
	 * @param exceptionType The type of exception to handle. Also matches an subclass of this type.
	 * @param onError       The {@link Consumer} to invoke when this error occurs.
	 * @param <E>           The type of exception.
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public <E extends Throwable> Composable<T> when(Class<E> exceptionType, final Consumer<E> onError) {
		Assert.notNull(exceptionType);
		Assert.notNull(onError);

		if (!isComplete()) {
			observable.on(Fn.T(exceptionType), new Consumer<Event<E>>() {
				@Override
				public void accept(Event<E> ev) {
					onError.accept(ev.getData());
				}
			});
		} else if(isError()){
			Fn.schedule(onError, (E)error, observable);
		}
		return this;
	}

	protected Registration<Consumer<Event<T>>> when(Selector sel, final Consumer<T> consumer) {
		if (!isComplete()) {
			return observable.on(sel, new Consumer<Event<T>>() {
				@Override
				public void accept(Event<T> ev) {
					consumer.accept(ev.getData());
				}
			});
		} else {
			Fn.schedule(consumer, value, observable);
		}
		return null;
	}

	protected Observable createObservable(Dispatcher dispatcher) {
		return new Reactor(dispatcher);
	}

	protected Observable createObservable(Observable src) {
		if (null == src) {
			return new Reactor();
		}
		if (src instanceof Reactor) {
			return new Reactor((Reactor) src, SYNCHRONOUS_DISPATCHER);
		} else {
			return new Reactor();
		}
	}

	protected <U> Composable<U> createComposable(Observable src) {
		Composable<U> c = new Composable<U>(src);
		c.expectedAcceptCount.set(expectedAcceptCount.get());
		return c;
	}

	protected void decreaseAcceptLength() {
		if (expectedAcceptCount.decrementAndGet() <= acceptedCount.get()) {
			synchronized (monitor) {
				monitor.notifyAll();
			}
		}
	}

	protected <V> void handleError(final Composable<V> c, Throwable t) {
		c.observable.notify(t.getClass(), Fn.event(t));
		c.decreaseAcceptLength();
	}

	/**
	 * Build a {@link Composable} based on the given values, {@link Dispatcher dispatcher}, and {@link Reactor reactor}.
	 *
	 * @param <T> The type of the values.
	 */
	public static class Builder<T> extends ReactorBuilder<Builder<T>, Composable<T>> {

		protected final Iterable<T> values;
		protected final Supplier<T> supplier;

		Builder(Iterable<T> values, Supplier<T> supplier) {
			this.values = values;
			this.supplier = supplier;
		}

		public Builder(Iterable<T> values) {
			this(values, null);
		}

		public Builder(Supplier<T> supplier) {
			this(null, supplier);
		}

		public Builder() {
			this(null, null);
		}

		@Override
		public Composable<T> doBuild(final Reactor reactor) {
			if (values != null) {
				return new DelayedAcceptComposable<T>(reactor, values);
			} else if (supplier != null) {
				return new DelayedAcceptComposable<T>(reactor, 1) {
					@Override
					protected void delayedAccept() {
						final DelayedAcceptComposable<T> self = this;
						Fn.schedule(new Consumer<Object>() {
							@Override
							public void accept(Object o) {
								try {
									self.doAccept(null, null, supplier.get());
								} catch (Throwable t) {
									self.doAccept(t, null, null);
								}
							}
						}, null, reactor);
					}
				};
			} else {
				return new DelayedAcceptComposable<T>(reactor, -1);
			}
		}
	}

	private static class DelayedAcceptComposable<T> extends Composable<T> {
		private final Object stateMonitor = new Object();
		protected final Iterable<T> values;
		protected AcceptState acceptState = AcceptState.DELAYED;

		protected DelayedAcceptComposable(Observable src, Iterable<T> values) {
			super(src);
			this.values = values;
			if (values instanceof Collection) {
				expectedAcceptCount.set(((Collection<?>) values).size());
			}
		}

		protected DelayedAcceptComposable(Observable src, long length) {
			super(src);
			expectedAcceptCount.set(length);
			this.values = null;
		}

		@Override
		public void accept(Throwable error) {
			synchronized (monitor) {
				this.error = error;
			}
			observable.notify(error.getClass(), Fn.event(error));
		}

		@Override
		public void accept(T value) {
			synchronized (monitor) {
				this.value = value;
			}
			acceptedCount.incrementAndGet();

			Event<T> ev = Fn.event(value);
			ev.getHeaders().set(EXPECTED_ACCEPT_LENGTH_HEADER, String.valueOf(expectedAcceptCount.get()));

			if (acceptedCount.get() == 1) {
				observable.notify(firstKey, ev);
			}

			observable.notify(acceptKey, ev);

			if (acceptedCount.get() == expectedAcceptCount.get()) {
				observable.notify(lastKey, ev);
				synchronized (monitor) {
					monitor.notifyAll();
				}
			}
		}

		@Override
		public T await(long timeout, TimeUnit unit) throws InterruptedException {
			delayedAccept();
			return super.await(timeout, unit);
		}

		@Override
		public T get() {
			delayedAccept();
			return super.get();
		}

		@Override
		protected <U> Composable<U> createComposable(Observable src) {
			final DelayedAcceptComposable<T> self = this;
			return new DelayedAcceptComposable<U>(src, self.expectedAcceptCount.get()) {
				@Override
				protected void delayedAccept() {
					self.delayedAccept();
				}
			};
		}

		protected void delayedAccept() {
			doAccept(null, null, null);
		}

		protected void doAccept(Throwable localError, Iterable<T> localValues, T localValue) {

			boolean acceptRequired = false;

			synchronized (this.stateMonitor) {
				if (acceptState == AcceptState.ACCEPTED) {
					return;
				} else if (acceptState == AcceptState.DELAYED) {
					if (localError == null && localValue == null && localValues == null) {
						synchronized (this.monitor) {
							localError = error;
							localValue = value;
							localValues = values;
						}
					}
					acceptState = AcceptState.ACCEPTING;
					acceptRequired = true;
				} else {
					while (acceptState == AcceptState.ACCEPTING) {
						try {
							stateMonitor.wait();
						} catch (InterruptedException ie) {
							Thread.currentThread().interrupt();
							break;
						}
					}
				}
			}

			if (acceptRequired) {
				if (null != localError) {
					accept(localError);
				} else if (null != localValues) {
					for (T t : localValues) {
						accept(t);
					}
				} else if (null != localValue) {
					accept(localValue);
				}
				synchronized (stateMonitor) {
					acceptState = AcceptState.ACCEPTED;
					stateMonitor.notifyAll();
				}
			}
		}

		private static enum AcceptState {
			DELAYED, ACCEPTING, ACCEPTED;
		}
	}

}