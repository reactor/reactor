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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Fn;
import reactor.fn.*;
import reactor.fn.dispatch.Dispatcher;

/**
 * A {@literal Promise} is a {@link Composable} that can only be used once. When created, it starts with a state of
 * {@link State#PENDING}. If a value of type {@link Throwable} is set, then the {@literal Promise} transitions to state
 * {@link State#FAILURE} and the error handlers are called. If a value of type <code>&lt;T&gt;</code> is set instead,
 * the {@literal Promise} transitions to state {@link State#SUCCESS}.
 * <p/>
 * Calls to {@link reactor.core.Promise#get()} are always non-blocking. If it is desirable to block the calling thread
 * until a result is available, though, call the {@link Promise#await(long, java.util.concurrent.TimeUnit)} method.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class Promise<T> extends Composable<T> {

	/**
	 * A {@literal Promise} can only be in state {@link State#PENDING}, {@link State#SUCCESS}, or {@link State#FAILURE}.
	 */
	public enum State {
		/**
		 * Means this {@literal Promise} has not been fulfilled yet.
		 */
		PENDING,
		/**
		 * Means this {@literal Promise} contains a value of type <code>&lt;T&gt;</code>
		 */
		SUCCESS,
		/**
		 * Means this {@literal Promise} contains an error. Any calls to {@link reactor.core.Promise#get()} will result in the
		 * error being thrown.
		 */
		FAILURE
	}

	private final Logger log = LoggerFactory.getLogger(getClass());

	private volatile State state = State.PENDING;

	/**
	 * Create a {@literal Promise} based on the given {@link Observable}.
	 *
	 * @param src The {@link Observable} to use when publishing events internally.
	 */
	public Promise(Observable src) {
		super(src);
		expectedAcceptCount.set(1);
	}

	/**
	 * Create a {@literal Promise} with default behavior.
	 */
	public Promise() {
		super();
		expectedAcceptCount.set(1);
		observable.on(Fn.T(Throwable.class), new Consumer<Event<Throwable>>() {
			@Override
			public void accept(Event<Throwable> throwableEvent) {
				if (state == State.PENDING) {
					Promise.this.set(throwableEvent.getData());
				} else {
					log.error(throwableEvent.getData().getMessage(), throwableEvent.getData());
				}
			}
		});
	}

	/**
	 * Create a {@literal Promise}.
	 *
	 * @param <T> The type of the value.
	 * @return The new {@literal Promise}.
	 */
	public static <T> Promise<T> create() {
		return new Promise<T>();
	}

	/**
	 * Create a {@literal Promise} that uses a synchronous {@link Dispatcher}.
	 *
	 * @param <T> The type of the value.
	 * @return The new {@literal Promise}.
	 * @see {@link reactor.core.Context#synchronousDispatcher()}
	 */
	public static <T> Promise<T> sync() {
		return new Promise<T>().setDispatcher(Context.synchronousDispatcher());
	}

	/**
	 * Create a {@literal Promise} that uses a synchronous {@link Dispatcher}.
	 *
	 * @param reason The error to set the value of this {@literal Promise} to.
	 * @param <T>    The type of the value.
	 * @return The new {@literal Promise}.
	 * @see {@link reactor.core.Context#synchronousDispatcher()}
	 */
	public static <T> Promise<T> sync(Throwable reason) {
		return Promise.<T>from(reason).setDispatcher(Context.synchronousDispatcher());
	}

	/**
	 * Create a {@literal Promise} using the given value which uses a synchronous {@link Dispatcher}.
	 *
	 * @param value The value of the {@literal Promise}.
	 * @param <T>   The type of the value.
	 * @return The new {@literal Promise}.
	 * @see {@link reactor.core.Context#synchronousDispatcher()}
	 */
	public static <T> Promise<T> sync(T value) {
		return from(value).setDispatcher(Context.synchronousDispatcher());
	}

	/**
	 * Create a {@literal Promise} based on the given exception.
	 *
	 * @param reason The exception to use as the value.
	 * @param <T>    The type of the intended {@literal Promise} value.
	 * @return The new {@literal Promise}.
	 */
	public static <T> Promise<T> from(Throwable reason) {
		return new Promise<T>().set(reason);
	}

	/**
	 * Create a {@literal Promise} based on the given value.
	 *
	 * @param value The value to use.
	 * @param <T>   The type of the value.
	 * @return The new {@literal Promise}.
	 */
	public static <T> Promise<T> from(T value) {
		return new Promise<T>().set(value);
	}

	@Override
	public Promise<T> setDispatcher(Dispatcher dispatcher) {
		super.setDispatcher(dispatcher);
		return this;
	}

	/**
	 * Get the state the {@literal Promise} is currently in.
	 *
	 * @return One of {@link State#PENDING}, {@link State#SUCCESS}, or {@link State#FAILURE}.
	 */
	public State getState() {
		return this.state;
	}

	/**
	 * Set this {@literal Promise} to state {@link State#FAILURE} and set the value of the {@literal Promise} so that
	 * subsequent calls to {@link reactor.core.Promise#get()} will throw this exception instead of returning a value.
	 *
	 * @param error The exception to use.
	 * @return {@literal this}
	 */
	public Promise<T> set(Throwable error) {
		synchronized (monitor) {
			assertPending();
			this.state = State.FAILURE;
		}
		super.accept(error);
		return this;
	}

	/**
	 * Set this {@literal Promise} to state {@link State#SUCCESS} and set the internal value to the given value.
	 *
	 * @param value The value to use.
	 * @return {@literal this}
	 */
	public Promise<T> set(T value) {
		synchronized (monitor) {
			assertPending();
			this.state = State.SUCCESS;
		}
		super.accept(value);
		return this;
	}

	/**
	 * Set a {@link Consumer} to invoke when this {@literal Promise} has either completed successfully or failed and set to
	 * an error.
	 *
	 * @param onComplete The {@link Consumer} to invoke on either failure or success.
	 * @return {@literal this}
	 */
	public Promise<T> onComplete(final Consumer<Promise<T>> onComplete) {
		onSuccess(new Consumer<T>() {
			@Override
			public void accept(T t) {
				onComplete.accept(Promise.this);
			}
		});
		onError(new Consumer<Throwable>() {
			@Override
			public void accept(Throwable t) {
				onComplete.accept(Promise.this);
			}
		});
		return this;
	}

	/**
	 * Set a {@link Consumer} to invoke when this {@literal Promise} has completed successfully.
	 *
	 * @param onSuccess The {@link Consumer} to invoke on success.
	 * @return {@literal this}
	 */
	public Promise<T> onSuccess(Consumer<T> onSuccess) {
		consume(onSuccess);
		return this;
	}

	/**
	 * Set a {@link Consumer} to invoke when this {@literal Promise} has failed.
	 *
	 * @param onError The {@link Consumer} to invoke on failure.
	 * @return {@literal this}
	 */
	public Promise<T> onError(Consumer<Throwable> onError) {
		when(Throwable.class, onError);
		return this;
	}

	/**
	 * Implementation of <a href="#">Promises/A+</a> that sets a success and failure {@link Consumer} in one call.
	 *
	 * @param onSuccess The {@link Consumer} to invoke on success.
	 * @param onError   The {@link Consumer} to invoke on failure.
	 * @return {@literal this}
	 */
	public Promise<T> then(Consumer<T> onSuccess, Consumer<Throwable> onError) {
		onSuccess(onSuccess);
		onError(onError);
		return this;
	}

	/**
	 * Implementation of <a href="#">Promises/A+</a> that sets a success and failure {@link Consumer} in one call and
	 * transforms the initial result into something else using the given {@link Function}.
	 *
	 * @param onSuccess The {@link Function} to invoke on success.
	 * @param onError   The {@link Consumer} to invoke on failure.
	 * @return {@literal this}
	 */
	public <V> Composable<V> then(Function<T, V> onSuccess, Consumer<Throwable> onError) {
		Composable<V> c = map(onSuccess);
		c.when(Throwable.class, onError);
		return c;
	}

	@Override
	public Composable<T> consume(final Consumer<T> consumer) {
		switch (state) {
			case SUCCESS: {
				R.schedule(consumer, value, observable);
				return this;
			}
			case FAILURE:
				assertSuccess();
			default:
				return super.consume(consumer);
		}
	}

	@Override
	public Composable<T> consume(Object key, Observable observable) {
		switch (state) {
			case SUCCESS: {
				observable.notify(key, Fn.event(value));
				return this;
			}
			case FAILURE:
				assertSuccess();
			default:
				return super.consume(key, observable);
		}
	}

	@Override
	public Composable<T> first() {
		switch (state) {
			case SUCCESS: {
				Composable<T> c = super.first();
				c.accept(value);
				return c;
			}
			case FAILURE:
				assertSuccess();
			default:
				return super.first();
		}
	}

	@Override
	public Composable<T> last() {
		switch (state) {
			case SUCCESS: {
				Composable<T> c = super.last();
				c.accept(value);
				return c;
			}
			case FAILURE:
				assertSuccess();
			default:
				return super.last();
		}
	}

	@Override
	public <V> Composable<V> map(final Function<T, V> fn) {
		switch (state) {
			case SUCCESS: {
				final Composable<V> c = createComposable(createObservable(observable));
				R.schedule(new Consumer<T>() {
					@Override
					public void accept(T value) {
						try {
							c.accept(fn.apply(value));
						} catch (Throwable t) {
							c.observable.notify(Fn.T(t.getClass()), Fn.event(t));
							c.decreaseAcceptLength();
						}
					}
				}, value, observable);
				return c;
			}
			case FAILURE:
				assertSuccess();
			default:
				return super.map(fn);
		}
	}

	@Override
	public Composable<T> filter(final Function<T, Boolean> fn) {
		switch (state) {
			case SUCCESS: {
				final Composable<T> c = createComposable(createObservable(observable));
				R.schedule(new Consumer<T>() {
					@Override
					public void accept(T value) {
						try {
							if (fn.apply(value)) {
								c.accept(value);
							} else {
								c.decreaseAcceptLength();
							}
						} catch (Throwable t) {
							c.observable.notify(Fn.T(t.getClass()), Fn.event(t));
							c.decreaseAcceptLength();
						}
					}
				}, value, observable);
				return c;
			}
			case FAILURE:
				assertSuccess();
			default:
				return super.filter(fn);
		}
	}

	public void accept(Throwable error) {
		set(error);
	}

	@Override
	public void accept(T value) {
		set(value);
	}

	@Override
	public T get() {
		assertSuccess();
		return super.get();
	}

	@Override
	protected <U> Composable<U> createComposable(Observable src) {
		return new Promise<U>(src);
	}

	private void assertSuccess() {
		if (state == State.FAILURE) {
			throw new IllegalStateException(error);
		}
	}

	private void assertPending() {
		if (state != State.PENDING) {
			throw new IllegalStateException("This Promise has already completed.");
		}
	}

}
