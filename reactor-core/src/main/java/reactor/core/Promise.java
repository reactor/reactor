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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A {@literal Promise} is a {@link Composable} that can only be used once. When created, it is pending. If a value of
 * type {@link Throwable} is set, then the {@literal Promise} is completed {@link #isError in error} and the error
 * handlers are called. If a value of type <code>&lt;T&gt;</code> is set instead, the {@literal Promise} is completed
 * {@link #isSuccess successfully}.
 * <p/>
 * Calls to {@link reactor.core.Promise#get()} are always non-blocking. If it is desirable to block the calling thread
 * until a result is available, though, call the {@link Promise#await(long, java.util.concurrent.TimeUnit)} method.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class Promise<T> extends Composable<T> {

	private final Logger log = LoggerFactory.getLogger(getClass());

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
		this((Dispatcher) null);
	}

	public Promise(Dispatcher dispatcher) {
		super(dispatcher);
		expectedAcceptCount.set(1);
		observable.on(Fn.T(Throwable.class), new Consumer<Event<Throwable>>() {
			@Override
			public void accept(Event<Throwable> throwableEvent) {
				synchronized (monitor) {
					if (!isComplete()) {
						Promise.this.set(throwableEvent.getData());
					} else {
						log.error(throwableEvent.getData().getMessage(), throwableEvent.getData());
					}
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
	 */
	public static <T> Promise<T> sync() {
		return new Promise<T>(SYNCHRONOUS_DISPATCHER);
	}

	/**
	 * Create a {@literal Promise} that uses a synchronous {@link Dispatcher}.
	 *
	 * @param reason The error to set the value of this {@literal Promise} to.
	 * @param <T>    The type of the value.
	 * @return The new {@literal Promise}.
	 */
	public static <T> Promise<T> sync(Throwable reason) {
		return Promise.<T>sync().set(reason);
	}

	/**
	 * Create a {@literal Promise} using the given value which uses a synchronous {@link Dispatcher}.
	 *
	 * @param value The value of the {@literal Promise}.
	 * @param <T>   The type of the value.
	 * @return The new {@literal Promise}.
	 */
	public static <T> Promise<T> sync(T value) {
		return Promise.<T>sync().set(value);
	}

	/**
	 * Create a {@literal Promise} based on the given exception.
	 *
	 * @param reason The exception to use as the value.
	 * @param <T>    The type of the intended {@literal Promise} value.
	 * @return The new {@literal Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise.Builder<T> from(Throwable reason) {
		return (Builder<T>) new Builder<Object>(Arrays.asList((Object) reason));
	}

	/**
	 * Create a {@literal Promise} based on the given value.
	 *
	 * @param value The value to use.
	 * @param <T>   The type of the value.
	 * @return The new {@literal Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise.Builder<T> from(T value) {
		return new Builder<T>(Arrays.asList(value));
	}

	/**
	 * Create a {@literal Promise} based on the given supplier.
	 *
	 * @param supplier The value to use.
	 * @param <T>   The type of the function result.
	 * @return The new {@literal Promise}.
	 */
	public static <T> Builder<T> from(Supplier<T>  supplier) {
		return new Builder<T>(supplier);
	}

	/**
	 * Set the value of the {@literal Promise} so that subsequent calls to {@link reactor.core.Promise#get()} will throw
	 * this exception instead of returning a value.
	 *
	 * @param error The exception to use.
	 * @return {@literal this}
	 */
	public Promise<T> set(Throwable error) {
		synchronized (monitor) {
			assertPending();
			super.accept(error);
		}
		return this;
	}

	/**
	 * Set this {@literal Promise} to the given value.
	 *
	 * @param value The value to set.
	 * @return {@literal this}
	 */
	public Promise<T> set(T value) {
		synchronized (monitor) {
			assertPending();
			super.accept(value);
		}
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
	public <V> Promise<V> then(Function<T, V> onSuccess, Consumer<Throwable> onError) {
		Promise<V> c = (Promise<V>) map(onSuccess);
		c.when(Throwable.class, onError);
		return c;
	}

	/**
	 * Indicates if this {@literal Promise} has been successfully completed
	 *
	 * @return {@literal true} if fulfilled successfully, otherwise {@literal false}.
	 */
	public boolean isSuccess() {
		return acceptCountReached();
	}

	/**
	 * Indicates if this {@literal Promise} has completed in error
	 *
	 * @return {@literal true} if the promise completed in error, otherwise {@literal false}.
	 */
	public boolean isError() {
		return super.isError();
	}

	/**
	 * Indicates if this {@literal Promise} is still pending
	 *
	 * @return {@literal true} if pending, otherwise {@literal false}.
	 */
	public boolean isPending() {
		return !isComplete();
	}

	@Override
	public Composable<T> consume(final Consumer<T> consumer) {
		synchronized (monitor) {
			if (isError()) {
				throw new IllegalStateException(error);
			} else if (acceptCountReached()) {
				R.schedule(consumer, value, observable);
				return this;
			} else {
				return super.consume(consumer);
			}
		}
	}

	@Override
	public Composable<T> consume(Object key, Observable observable) {
		synchronized (monitor) {
			if (isError()) {
				throw new IllegalStateException(error);
			} else if (acceptCountReached()) {
				observable.notify(key, Fn.event(value));
				return this;
			} else {
				return super.consume(key, observable);
			}
		}
	}

	@Override
	public Composable<T> first() {
		synchronized (monitor) {
			if (isError()) {
				throw new IllegalStateException(error);
			} else if (acceptCountReached()) {
				Composable<T> c = super.first();
				c.accept(value);
				return c;
			} else {
				return super.first();
			}
		}
	}

	@Override
	public Composable<T> last() {
		synchronized (monitor) {
			if (isError()) {
				throw new IllegalStateException(error);
			} else if (acceptCountReached()) {
				Composable<T> c = super.last();
				c.accept(value);
				return c;
			} else {
				return super.last();
			}
		}
	}

	@Override
	public <V> Composable<V> map(final Function<T, V> fn) {
		synchronized (monitor) {
			if (isError()) {
				throw new IllegalStateException(error);
			} else if (acceptCountReached()) {
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
			} else {
				return super.map(fn);
			}
		}
	}

	@Override
	public Composable<T> filter(final Function<T, Boolean> fn) {
		synchronized (monitor) {
			if (isError()) {
				throw new IllegalStateException(error);
			} else if (acceptCountReached()) {
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
			} else {
				return super.filter(fn);
			}
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
		synchronized (this.monitor) {
			if (isError()) {
				throw new IllegalStateException(error);
			}
			return super.get();
		}
	}

	@Override
	protected <U> Composable<U> createComposable(Observable src) {
		return new Promise<U>(src);
	}

	private void assertPending() {
		synchronized (monitor) {
			if (!isPending()) {
				throw new IllegalStateException("This Promise has already completed.");
			}
		}
	}

	/**
	 * Build a {@link Composable} based on the given values, {@link Dispatcher dispatcher}, and {@link Reactor reactor}.
	 *
	 * @param <T> The type of the values.
	 */
	public static class Builder<T> extends Composable.Builder<T> {

		Builder(Iterable<T> values) {
			super(values);
		}
		Builder(Supplier<T> values) {
			super(values);
		}

		@Override
		public Builder<T> using(Reactor reactor) {
			super.using(reactor);
			return this;
		}

		@Override
		public Builder<T> using(Dispatcher dispatcher) {
			super.using(dispatcher);
			return this;
		}

		public Promise<T> build() {
			configure();
			T value = null;
			if (List.class.isInstance(values)) {
				List<T> l = (List<T>) values;
				if (!l.isEmpty()) {
					value = ((List<T>) values).get(0);
				}
			} else if(values != null){
				value = values.iterator().next();
			}
			if (Throwable.class.isInstance(value)) {
				return new Promise<T>(reactor).set((Throwable) value);
			} else if(supplier != null){
				final Composable<T> composable = Composable.from(supplier).build();
				Promise<T> result = new Promise<T>(reactor){
					@Override
					public T get() {
						composable.get();
						return super.get();
					}

					@Override
					public T await(long timeout, TimeUnit unit) throws InterruptedException {
						composable.await(timeout, unit);
						return super.await(timeout, unit);
					}
				};
				composable.consume(result);
				return result;
			}else{
				return new Promise<T>(reactor).set(value);
			}
		}
	}

}
