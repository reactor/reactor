package reactor.core;

import reactor.fn.*;
import reactor.fn.registry.Registration;
import reactor.fn.selector.Selector;
import reactor.util.Assert;

import java.util.concurrent.TimeUnit;

import static reactor.fn.Functions.$;

/**
 * @author Stephane Maldini
 */
public abstract class Composable<T> implements Consumer<T>, Supplier<T> {

	protected final Object monitor = new Object();

	private final Object   acceptKey      = new Object();
	private final Selector acceptSelector = $(acceptKey);

	private final Environment env;
	private final Observable  observable;

	private long    acceptedCount       = 0L;
	private long    expectedAcceptCount = -1L;
	private boolean hasBlockers         = false;

	private T         value;
	private Throwable error;


	/**
	 * Create a {@link Composable} that uses the given {@link reactor.core.Reactor} for publishing events internally.
	 *
	 * @param observable The {@link reactor.core.Reactor} to use.
	 */
	protected Composable(Environment env, Observable observable) {
		Assert.notNull(observable, "Observable cannot be null.");
		this.env = env;
		this.observable = observable;
	}

	protected <U> Composable<U> assignComposable(Observable src) {
		Composable<U> c = this.createComposable(src);
		synchronized (monitor) {
			c.doSetExpectedAcceptCount(getExpectedAcceptCount());
		}
		forwardError(c);
		return c;
	}

	abstract protected <U> Composable<U> createComposable(Observable src);

	/**
	 * Set the number of times to expect {@link #accept(Object)} to be called.
	 *
	 * @param expectedAcceptCount The number of times {@link #accept(Object)} will be called.
	 * @return {@literal this}
	 */
	public Composable<T> setExpectedAcceptCount(long expectedAcceptCount) {
		synchronized (monitor) {
			doSetExpectedAcceptCount(expectedAcceptCount);
			if (acceptCountReached()) {
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
	 * Register a {@code key} and {@link reactor.core.Reactor} on which to publish an event whenever {@link #accept(Object)} is called.
	 *
	 * @param key        The key to use when publishing the {@link reactor.fn.Event}.
	 * @param observable The {@link Observable} on which to publish the {@link reactor.fn.Event}.
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
	 * Create a new {@link Composable} that is linked to the parent through the given {@link reactor.fn.Function}. When the parent's
	 * {@link #accept(Object)} is invoked, this {@link reactor.fn.Function} is invoked and the result is passed into the returned
	 * {@link Composable}.
	 *
	 * @param fn  The transformation function to apply.
	 * @param <V> The type of the object returned when the given {@link reactor.fn.Function}.
	 * @return The new {@link Composable}.
	 */
	public <V> Composable<V> map(final Function<T, V> fn) {
		Assert.notNull(fn);
		final Composable<V> c = this.assignComposable(observable);
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
	 * Selectively call the returned {@link Stream} depending on the predicate {@link Function} argument
	 *
	 * @param fn The filter function, taking argument {@param <T>} and returning a {@link Boolean}
	 * @return The new {@link Stream}.
	 */
	public Composable<T> filter(final Function<T, Boolean> fn) {
		Assert.notNull(fn);
		final Composable<T> c = this.assignComposable(observable);
		consume(new Consumer<T>() {
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
		synchronized (monitor) {
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

	protected final void notifyAccept(Event<?> event) {
		observable.notify(acceptKey, event);
	}

	protected void decreaseAcceptLength() {
		synchronized (monitor) {
			if (--expectedAcceptCount <= acceptedCount) {
				monitor.notifyAll();
			}
		}
	}

	protected void handleError(Composable<?> c, Throwable t) {
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
}
