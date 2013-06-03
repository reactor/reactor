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

import reactor.fn.*;
import reactor.fn.dispatch.Dispatcher;
import reactor.fn.selector.Selector;
import reactor.fn.support.Reduce;
import reactor.util.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static reactor.fn.Functions.$;

/**
 * A {@literal Composable} is a way to provide components when other threads to act on incoming data and provide new
 * data to other components that must wait on the data to become available.
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public class Composable<T> implements Consumer<T>, Supplier<T> {

	protected final Object monitor = new Object();

	private final Object   acceptKey      = new Object();
	private final Selector acceptSelector = $(acceptKey);

	private final Object   firstKey      = new Object();
	private final Selector firstSelector = $(firstKey);

	private final Object   lastKey      = new Object();
	private final Selector lastSelector = $(lastKey);

	private final Environment env;
	private final Observable  observable;

	private long      acceptedCount       = 0L;
	private long      expectedAcceptCount = -1L;
	private boolean   hasBlockers         = false;

	private T         value;
	private Throwable error;

	/**
	 * Create a {@link Composable} that uses the given {@link Reactor} for publishing events internally.
	 *
	 * @param observable The {@link Reactor} to use.
	 */
	Composable(Environment env, Observable observable) {
		Assert.notNull(observable, "Observable cannot be null.");
		this.env = env;
		this.observable = observable;
	}

	/**
	 * Set the number of times to expect {@link #accept(Object)} to be called.
	 *
	 * @param expectedAcceptCount The number of times {@link #accept(Object)} will be called.
	 * @return {@literal this}
	 */
	public Composable<T> setExpectedAcceptCount(long expectedAcceptCount) {
		boolean notifyLast = false;
		synchronized (monitor) {
			doSetExpectedAcceptCount(expectedAcceptCount);
			if (acceptCountReached()) {
				monitor.notifyAll();
				notifyLast = true;
			}
		}

		if (notifyLast) {
			observable.notify(lastKey, Event.wrap(value));
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
	 * Register a {@link Composable} that will be invoked whenever {@link #accept(Object)} or {@link #accept (Throwable)}
	 * are called.
	 *
	 * @param composable The composable to invoke.
	 * @return {@literal this}
	 * @see {@link #accept(Object)}
	 */
	public Composable<T> consume(Composable<T> composable) {
		when(acceptSelector, composable);
		forwardError(composable);
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
				observable.notify(key, Event.class.isAssignableFrom(event.getClass()) ? (Event<?>) event : Event.wrap(event));
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
		c.doSetExpectedAcceptCount(1);

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
		c.doSetExpectedAcceptCount(1);

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
		final Composable<V> c = createComposable(observable);
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
	 * {@link Observable#notify (reactor.Event.wrap)} along with the given {@code key}. After the event is being propagated
	 * to the reactor consumers, the new composition expects {@param <V>} replies to be returned.
	 *
	 * @param key        The key to notify
	 * @param observable The observable to notify
	 * @param <V>        The type of the object returned by reactor reply.
	 * @return The new {@link Composable}.
	 */
	public <V> Composable<V> map(final Object key, final Observable observable) {
		Assert.notNull(observable);
		final Composable<V> c = createComposable(observable);
		c.setExpectedAcceptCount(-1);
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
					Event<?> event = Event.class.isAssignableFrom(value.getClass()) ? (Event<?>) value : Event.wrap(value);
					event.setReplyTo(replyTo);
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
	 * result will notify the returned {@link Composable}. A {@link Function} taking a {@link reactor.fn.support.Reduce}
	 * argument must be passed to process each pair formed of the last accumulated result and a new value to be processed.
	 *
	 * @param fn      The reduce function
	 * @param initial The initial accumulated result value e.g. an empty list.
	 * @param <V>     The type of the object returned by reactor reply.
	 * @return The new {@link Composable}.
	 */
	public <V> Composable<V> reduce(final Function<Reduce<T, V>, V> fn, V initial) {
		Assert.notNull(fn);
		final AtomicReference<V> lastValue = new AtomicReference<V>(initial);
		final Composable<V> c = createComposable(observable);

		final long _expectedAcceptCount;
		synchronized(monitor) {
			_expectedAcceptCount = expectedAcceptCount;
		}

		c.setExpectedAcceptCount(_expectedAcceptCount < 0 ? _expectedAcceptCount : 1);
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
					if (_expectedAcceptCount < 0) {
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
	 * result will notify the returned {@link Composable}. Will automatically generate a collection formed from composable
	 * streamed results, until accept count is reached.
	 *
	 * @return The new {@link Composable}.
	 */
	public Composable<List<T>> reduce() {
		return reduce(new Function<Reduce<T, List<T>>, List<T>>() {
			@Override
			public List<T> apply(Reduce<T, List<T>> reducer) {
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
	public Composable<T> take(long count) {
		final AtomicLong cursor = new AtomicLong(count);
		final AtomicReference<Registration<Consumer<Event<T>>>> reg = new
				AtomicReference<Registration<Consumer<Event<T>>>>();
		final Composable<T> c = createComposable(observable);
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
		final Composable<T> c = createComposable(observable);
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
		observable.notify(error.getClass(), Event.wrap(error));
	}

	/**
	 * Trigger composition with a value to be processed by dedicated consumers
	 *
	 * @param value The exception
	 */
	@Override
	public void accept(T value) {
		synchronized (monitor) {
			setValue(value);
			this.value = value;
			if (hasBlockers) {
				monitor.notifyAll();
			}
		}
		notifyAccept(Event.wrap(value));
	}

	public T await() throws InterruptedException {
		long defaultTimeout = 30000L;
		if (null != env) {
			defaultTimeout = env.getProperty("reactor.await.defaultTimeout", Long.class, defaultTimeout);
		}
		return await(defaultTimeout, TimeUnit.MILLISECONDS);
	}

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
		synchronized(monitor) {
			return expectedAcceptCount >= 0 && acceptedCount >= expectedAcceptCount;
		}
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
			observable.on(Functions.T(exceptionType), new Consumer<Event<E>>() {
				@Override
				public void accept(Event<E> ev) {
					onError.accept(ev.getData());
				}
			});
		} else if (isError()) {
			Functions.schedule(onError, (E) error, observable);
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
		} else if (!isError()) {
			Functions.schedule(consumer, value, observable);
		}
		return null;
	}

	protected Composable<T> forwardError(final Composable<?> composable) {
		if (composable.observable == observable) {
			return this;
		}
		when(Throwable.class, new Consumer<Throwable>() {
			@Override
			public void accept(Throwable t) {
				composable.accept(t);
				composable.decreaseAcceptLength();
			}
		});
		return this;
	}

	protected Reactor createReactor(Observable src) {
		Reactor.Spec rspec = Reactors.reactor().using(env);

		if (null != src && Reactor.class.isInstance(src)) {
			rspec.using((Reactor) src);
		}

		return rspec.sync().get();
	}

	protected <U> Composable<U> createComposable(Observable src) {
		Composable<U> c = new Composable<U>(env, createReactor(src));
		synchronized(monitor) {
			c.doSetExpectedAcceptCount(expectedAcceptCount);
		}
		forwardError(c);
		return c;
	}

	protected void decreaseAcceptLength() {
		synchronized (monitor) {
			if (--expectedAcceptCount <= acceptedCount) {
				monitor.notifyAll();
			}
		}
	}

	protected <V> void handleError(Composable<V> c, Throwable t) {
		c.observable.notify(t.getClass(), Event.wrap(t));
		c.decreaseAcceptLength();
	}

	protected final T getValue() {
		synchronized (this.monitor) {
			return this.value;
		}
	}

	protected final void setValue(T value) {
		synchronized (monitor) {
			this.value = value;
			acceptedCount++;
		}
	}

	protected final boolean isFirst() {
		synchronized (monitor) {
			return acceptedCount == 1;
		}
	}

	protected final Throwable getError() {
		synchronized (monitor) {
			return this.error;
		}
	}

	protected final void setError(Throwable error) {
		synchronized (monitor) {
			this.error = error;
		}
	}

	protected final void notifyError(Throwable error) {
		synchronized (monitor) {
			observable.notify(error.getClass(), Event.wrap(error));
		}
	}

	protected final void notifyFirst(Event<?> event) {
		observable.notify(firstKey, event);
	}

	protected final void notifyAccept(Event<?> event) {
		observable.notify(acceptKey, event);
	}

	protected final void notifyLast(Event<?> event) {
		observable.notify(lastKey, event);
	}

	protected final Environment getEnvironment() {
		return env;
	}

	protected final void doSetExpectedAcceptCount(long expectedAcceptCount) {
		synchronized (monitor) {
			this.expectedAcceptCount = expectedAcceptCount;
		}
	}

	protected long getExpectedAcceptCount() {
		synchronized (monitor) {
			return expectedAcceptCount;
		}
	}

	protected final Observable getObservable() {
		return this.observable;
	}

	/**
	 * Build a {@link Composable} based on the given values, {@link Dispatcher dispatcher}, and {@link Reactor reactor}.
	 *
	 * @param <T> The type of the values.
	 */
	public static class Spec<T> extends ComponentSpec<Spec<T>, Composable<T>> {

		protected final Iterable<T> values;

		public Spec(Iterable<T> values) {
			this.values = values;
		}

		@Override
		protected Composable<T> configure(final Reactor reactor) {

			final Composable<T> comp;
			if (values != null) {
				comp = new DelayedAcceptComposable<T>(env, reactor, values);
			} else {
				comp = new DelayedAcceptComposable<T>(env, reactor, -1);
			}
			return comp;
		}
	}

	protected static class DelayedAcceptComposable<T> extends Composable<T> {
		private final Object stateMonitor = new Object();
		protected final Iterable<T> values;
		protected AcceptState acceptState = AcceptState.DELAYED;

		protected DelayedAcceptComposable(Environment env, Observable src, Iterable<T> values) {
			super(env, src);
			this.values = values;
			if (values instanceof Collection) {
				setExpectedAcceptCount((((Collection<?>) values).size()));
			}
		}

		protected DelayedAcceptComposable(Environment env, Observable src, long length) {
			super(env, src);
			this.values = null;
			setExpectedAcceptCount(length);
		}

		@Override
		public void accept(Throwable error) {
			setError(error);
			notifyError(error);
		}

		@Override
		public void accept(T value) {
			boolean notifyFirst = false;
			boolean notifyLast = false;

			synchronized(monitor) {
				setValue(value);

				if (isFirst()) {
					notifyFirst = true;
				}

				if (acceptCountReached()) {
					notifyLast = true;
					monitor.notifyAll();
				}
			}

			Event<T> ev = Event.wrap(value);

			if (notifyFirst) {
				notifyFirst(ev);
			}

			notifyAccept(ev);

			if (notifyLast) {
				notifyLast(ev);
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
			final DelayedAcceptComposable<U> c =
					new DelayedAcceptComposable<U>(getEnvironment(), createReactor(src), self.getExpectedAcceptCount()) {
						@Override
						protected void delayedAccept() {
							self.delayedAccept();
						}
					};
			forwardError(c);
			return c;
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
							localError = getError();
							localValue = getValue();
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
			DELAYED, ACCEPTING, ACCEPTED
		}
	}

}