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
import reactor.fn.tuples.Tuple2;
import reactor.util.Assert;

import java.util.Arrays;
import java.util.Collection;

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

	Promise(Environment env, Observable src) {
		super(env, src);
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
		Assert.notNull(onComplete);
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
		Assert.notNull(onSuccess);
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
		Assert.notNull(onError);
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
		if (null != onError)
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
		Promise<V> c = map(onSuccess);
		if (null != onError)
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
	public Promise<T> consume(final Consumer<T> consumer) {
		synchronized (monitor) {
			if (isError()) {
				return this;
			} else if (acceptCountReached()) {
				Fn.schedule(consumer, value, observable);
				return this;
			} else {
				return (Promise<T>) super.consume(consumer);
			}
		}
	}

	@Override
	public Promise<T> consume(Object key, Observable observable) {
		synchronized (monitor) {
			if (acceptCountReached()) {
				observable.notify(key, Fn.event(value));
				return this;
			} else {
				return (Promise<T>) super.consume(key, observable);
			}
		}
	}

	@Override
	public Promise<T> first() {
		synchronized (monitor) {
			if (acceptCountReached()) {
				Promise<T> c = (Promise<T>) super.first();
				c.set(value);
				return c;
			} else {
				return (Promise<T>) super.first();
			}
		}
	}

	@Override
	public Promise<T> last() {
		synchronized (monitor) {
			if (acceptCountReached()) {
				Promise<T> c = (Promise<T>) super.last();
				c.set(value);
				return c;
			} else {
				return (Promise<T>) super.last();
			}
		}
	}

	@Override
	public <V> Promise<V> map(final Function<T, V> fn) {
		synchronized (monitor) {
			if (acceptCountReached()) {
				final Promise<V> c = createComposable(observable);
				Fn.schedule(new Consumer<T>() {
					@Override
					public void accept(T value) {
						try {
							c.accept(fn.apply(value));
						} catch (Throwable t) {
							handleError(c, t);
						}
					}
				}, value, observable);
				return c;
			} else {
				return (Promise<V>) super.map(fn);
			}
		}
	}

	@Override
	public Promise<T> filter(final Function<T, Boolean> fn) {
		synchronized (monitor) {
			if (acceptCountReached()) {
				final Promise<T> c = createComposable(observable);
				Fn.schedule(new Consumer<T>() {
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
				}, value, observable);
				return c;
			} else {
				return (Promise<T>) super.filter(fn);
			}
		}
	}

	@Override
	protected <V> void handleError(Composable<V> c, Throwable t) {
		c.accept(t);
		c.decreaseAcceptLength();
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
	protected <U> Promise<U> createComposable(Observable src) {
		final Promise<U> p = new Promise<U>(env, src);
		forwardError(p);
		return p;
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
	public static class Spec<T> extends AbstractComposableSpec<T, Promise<T>, Spec<T>> {
		@SuppressWarnings("unchecked")
		public Spec(T value, Supplier<T> supplier, Throwable error) {
			super((null != value ? Arrays.asList(value) : null), supplier, error);
		}

		private T getValue() {
			if (null != values) {
				return values.iterator().next();
			} else {
				return null;
			}
		}

		@Override
		@SuppressWarnings("unchecked")
		protected Promise<T> configure(Reactor reactor) {
			if (null != mergeWith) {
				//TODO fix generic hell
				final Promise<Collection<?>> p = (Promise) new Promise<Collection<?>>(env, reactor);
				doMerge(new DelayedAcceptComposable<Tuple2<?, Integer>>(env, reactor, mergeWith.size()))
						.consume(p);

				return (Promise<T>) p;
			} else {
				final Promise<T> prom;
				if (null != error) {
					prom = new Promise<T>(env, reactor).set(error);
				} else if (supplier != null) {
					prom = new Promise<T>(env, reactor);
					Fn.schedule(new Consumer<Object>() {
						@Override
						public void accept(Object o) {
							try {
								prom.set(supplier.get());
							} catch (Throwable t) {
								prom.set(t);
							}
						}
					}, null, reactor);
				} else if (null != values) {
					prom = new Promise<T>(env, reactor).set(getValue());
				} else {
					prom = new Promise<T>(env, reactor);
				}

				if (null != src) {
					src.consume(new Consumer<T>() {
						@Override
						public void accept(T t) {
							prom.accept(t);
						}
					});
				}

				return prom;
			}
		}
	}

}
