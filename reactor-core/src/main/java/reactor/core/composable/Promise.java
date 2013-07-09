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

import static reactor.function.Functions.$;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.event.Event;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.selector.Selector;
import reactor.event.support.EventConsumer;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Functions;
import reactor.function.Observable;
import reactor.function.Predicate;
import reactor.function.Supplier;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

/**
 * A {@code Promise} is a stateful event processor that accepts a single value and always exists in one of three states:
 * {@link Promise.State#PENDING PENDING}, {@link Promise.State#SUCCESS SUCCESS}, or {@link
 * Promise.State#FAILURE FAILURE}. It also provides methods for composing actions with the future value
 * much like a {@link Stream}. Where a {@link Stream} can process many values, a {@code Promise} processes only one (or
 * an error instead of a value).
 * <p/>
 * Reactor's {@code Promise} implementation is modeled largely after the <a href="https://github.com/promises-aplus/promises-spec">Promises/A+
 * specification</a>, which defines a number of methods and potential actions for promises.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 * @see <a href="https://github.com/promises-aplus/promises-spec">Promises/A+ specification</a>
 */
public class Promise<T> extends Composable<T> implements Supplier<T> {

	private final Object                   monitor  = new Object();
	private final Tuple2<Selector, Object> complete = $();

	private final long defaultTimeout;

	private volatile State state = State.PENDING;
	private T           value;
	private Throwable   error;
	private Supplier<T> supplier;
	private boolean hasBlockers = false;

	public Promise(@Nullable Environment env,
								 @Nonnull Observable events,
								 @Nullable Composable<?> parent,
								 Supplier<T> value,
								 Throwable error,
								 Supplier<T> supplier) {
		super(env, events, parent);
		this.defaultTimeout = env != null ? env.getProperty("reactor.await.defaultTimeout", Long.class, 30000L) : 30000L;
		if (null != value) {
			this.value = value.get();
			this.state = State.SUCCESS;
		} else if (null != error) {
			this.error = error;
			this.state = State.FAILURE;
		} else if (null != supplier) {
			this.supplier = supplier;
		}
	}

	/**
	 * Assign a {@link Consumer} that will either be invoked later, when the {@code Promise} is completed by either setting
	 * a value or propagating an error, or, if this {@code Promise} has already been fulfilled, is immediately scheduled to
	 * be executed on the current {@link reactor.event.dispatch.Dispatcher}.
	 *
	 * @param onComplete the completion {@link Consumer}
	 * @return {@literal this}
	 */
	@SuppressWarnings("unchecked")
	public Promise<T> onComplete(@Nonnull final Consumer<Promise<T>> onComplete) {
		if (isComplete()) {
			Functions.schedule(onComplete, this, getObservable());
		} else {
			getObservable().on(complete.getT1(), new EventConsumer<Promise<T>>(onComplete));
		}
		return this;
	}

	/**
	 * Assing a {@link Consumer} that will either be invoked later, when the {@code Promise} is successfully completed with
	 * a value, or, if this {@code Promise} has already been fulfilled, is immediately scheduled to be executed on the
	 * current {@link reactor.event.dispatch.Dispatcher}.
	 *
	 * @param onSuccess the success {@link Consumer}
	 * @return {@literal this}
	 */
	public Promise<T> onSuccess(@Nonnull final Consumer<T> onSuccess) {
		return consume(onSuccess);
	}

	/**
	 * Assing a {@link Consumer} that will either be invoked later, when the {@code Promise} is completed with an error,
	 * or, if this {@code Promise} has already been fulfilled, is immediately scheduled to be executed on the current
	 * {@link reactor.event.dispatch.Dispatcher}.
	 *
	 * @param onError the error {@link Consumer}
	 * @return {@literal this}
	 */
	public Promise<T> onError(@Nullable final Consumer<Throwable> onError) {
		if (null != onError) {
			return when(Throwable.class, onError);
		} else {
			return this;
		}
	}

	/**
	 * Assign both a success {@link Consumer} and an optional (possibly {@code null}) error {@link Consumer} in a single
	 * method call.
	 *
	 * @param onSuccess the success {@link Consumer}
	 * @param onError   the error {@link Consumer}
	 * @return {@literal this}
	 * @see {@link #onSuccess(reactor.function.Consumer)}
	 * @see {@link #onError(reactor.function.Consumer)}
	 */
	public Promise<T> then(@Nonnull Consumer<T> onSuccess, @Nullable Consumer<Throwable> onError) {
		onSuccess(onSuccess);
		onError(onError);
		return this;
	}

	/**
	 * Assign a success {@link Function} that will either be invoked later, when the {@code Promise} is successfully
	 * completed with a value, or, if this {@code Promise} has already been fulfilled, the function is immediately
	 * scheduled to be executed on the current {@link reactor.event.dispatch.Dispatcher}.
	 * <p/>
	 * A new {@code Promise} is returned that will be populated by result of the given transformation {@link Function} that
	 * turns the incoming {@code T} into a {@code V}.
	 *
	 * @param onSuccess the success transformation {@link Function}
	 * @param onError   the error {@link Consumer}
	 * @param <V>       the type of the value returned by the transformation {@link Function}
	 * @return a new {@code Promise} that will be populated by the result of the transformation {@link Function}
	 */
	@SuppressWarnings("unchecked")
	public <V> Promise<V> then(@Nonnull final Function<T, V> onSuccess, @Nullable final Consumer<Throwable> onError) {
		final Deferred<V, Promise<V>> d = createDeferred();

		Promise<V> p = d.compose().onError(onError);
		onSuccess(new Consumer<T>() {
			@Override
			public void accept(T value) {
				try {
					d.accept(onSuccess.apply(value));
				} catch (Throwable throwable) {
					d.accept(throwable);
				}
			}
		});
		onError(new Consumer<Throwable>() {
			@Override
			public void accept(Throwable t) {
				d.accept(t);
			}
		});
		return p;
	}

	/**
	 * Indicates whether this {@code Promise} has been completed with either an error or a value and is no longer {@code
	 * PENDING}.
	 *
	 * @return {@code true} if this {@code Promise} is complete, {@code false} otherwise.
	 * @see {@link State#PENDING}
	 * @see {@link State#SUCCESS}
	 * @see {@link State#FAILURE}
	 */
	public boolean isComplete() {
		lock.lock();
		try {
			return state != State.PENDING;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Indicates whether this {@code Promise} is still {@code PENDING} a value or error.
	 *
	 * @return {@code true} if this {@code Promise} is still pending, {@code false} otherwise.
	 * @see {@link State#PENDING}
	 */
	public boolean isPending() {
		lock.lock();
		try {
			return state == State.PENDING;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Indicates whether this {@code Promise} has been successfully completed a value and is no longer {@code PENDING}.
	 *
	 * @return {@code true} if this {@code Promise} is successful, {@code false} otherwise.
	 * @see {@link State#SUCCESS}
	 */
	public boolean isSuccess() {
		lock.lock();
		try {
			return state == State.SUCCESS;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Indicates whether this {@code Promise} has been completed an error and is no longer {@code PENDING}.
	 *
	 * @return {@code true} if this {@code Promise} is <strong>not</strong> successful, {@code false} otherwise.
	 * @see {@link State#FAILURE}
	 */
	public boolean isError() {
		lock.lock();
		try {
			return state == State.FAILURE;
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Block the calling thread, waiting for the completion of this {@code Promise}. A default timeout as specified in
	 * Reactor's {@link Environment} properties using the key {@code reactor.await.defaultTimeout} is used. The default is
	 * 30 seconds.
	 *
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has not
	 *         completed
	 * @throws InterruptedException
	 */
	public T await() throws InterruptedException {
		return await(defaultTimeout, TimeUnit.MILLISECONDS);
	}

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this {@code Promise}.
	 *
	 * @param timeout the timeout value
	 * @param unit    the {@link TimeUnit} of the timeout value
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has not
	 *         completed
	 * @throws InterruptedException
	 */
	public T await(long timeout, TimeUnit unit) throws InterruptedException {
		if (isPending()) {
			resolve();
		}

		if (!isPending()) {
			return get();
		}

		hasBlockers = true;
		synchronized (monitor) {
			if (timeout >= 0) {
				long msTimeout = TimeUnit.MILLISECONDS.convert(timeout, unit);
				long endTime = System.currentTimeMillis() + msTimeout;
				while (state == State.PENDING && (System.currentTimeMillis()) < endTime) {
					this.monitor.wait(200);
				}
			} else {
				while (state == State.PENDING) {
					this.monitor.wait(200);
				}
			}
		}
		hasBlockers = false;

		return get();
	}

	@Override
	public T get() {
		if (isPending()) {
			resolve();
		}
		if (isSuccess()) {
			return value;
		} else if (isError()) {
			if (RuntimeException.class.isInstance(error)) {
				throw (RuntimeException) error;
			} else {
				throw new RuntimeException(error);
			}
		} else {
			return null;
		}
	}

	/**
	 * Return the error (if any) that has completed this {@code Promise}.
	 *
	 * @return the error (if any)
	 */
	public Throwable reason() {
		if (isError()) {
			return error;
		} else {
			return null;
		}
	}

	@Override
	public Promise<T> consume(@Nonnull Consumer<T> consumer) {
		if (isSuccess()) {
			Functions.schedule(consumer, value, getObservable());
		} else {
			super.consume(consumer);
		}
		return this;
	}

	@Override
	public Promise<T> consume(@Nonnull final Composable<T> composable) {
		if (isSuccess()) {
			Functions.schedule(new Consumer<T>() {
				@Override
				public void accept(T t) {
					composable.notifyValue(t);
				}
			}, value, getObservable());
		} else {
			super.consume(composable);
		}
		return this;
	}

	@Override
	public Promise<T> consume(@Nonnull Object key, @Nonnull Observable observable) {
		if (isSuccess()) {
			observable.notify(key, Event.wrap(value));
		} else {
			super.consume(key, observable);
		}
		return this;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <E extends Throwable> Promise<T> when(@Nonnull Class<E> exceptionType, @Nonnull Consumer<E> onError) {
		if (isError() && exceptionType.isAssignableFrom(error.getClass())) {
			Functions.schedule(onError, (E) error, getObservable());
		} else {
			super.when(exceptionType, onError);
		}
		return this;
	}

	@Override
	public <V> Promise<V> map(@Nonnull final Function<T, V> fn) {
		if (isPending()) {
			return (Promise<V>) super.map(fn);
		}

		final Deferred<V, Promise<V>> d = createDeferred();
		if (isSuccess()) {
			Functions.schedule(
					new Consumer<Void>() {
						@Override
						public void accept(Void aVoid) {
							try {
								d.accept(fn.apply(value));
							} catch (Throwable throwable) {
								d.accept(throwable);
							}
						}
					},
					null,
					getObservable()
			);
		} else if (isError()) {
			d.accept(error);
		}
		return d.compose();
	}

	@Override
	public Promise<T> filter(@Nonnull final Predicate<T> p) {
		if (isPending()) {
			return (Promise<T>) super.filter(p);
		}

		final Deferred<T, Promise<T>> d = createDeferred();
		if (isSuccess()) {
			Functions.schedule(
					new Consumer<Void>() {
						@Override
						public void accept(Void aVoid) {
							try {
								if (p.test(value)) {
									d.accept(value);
								} else {
									d.accept(new IllegalArgumentException(String.format("%s failed a predicate test.", value)));
								}
							} catch (Throwable throwable) {
								d.accept(throwable);
							}
						}
					},
					null,
					getObservable()
			);
		} else if (isError()) {
			d.accept(error);
		}
		return d.compose();
	}

	@Override
	public Promise<T> resolve() {
		return (Promise<T>) super.resolve();
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <V, C extends Composable<V>> Deferred<V, C> createDeferred() {
		return (Deferred<V, C>) new Deferred<V, Promise<V>>(new Promise<V>(getEnvironment(), new Reactor(getEnvironment(), new SynchronousDispatcher()), this, null, null, null));
	}

	@Override
	protected void errorAccepted(Throwable error) {
		lock.lock();
		try {
			assertPending();
			this.state = State.FAILURE;
			this.error = error;
			if (hasBlockers) {
				monitor.notifyAll();
			}
		} finally {
			lock.unlock();
		}
		getObservable().notify(complete.getT2(), Event.wrap(this));

	}

	@Override
	protected void valueAccepted(T value) {
		lock.lock();
		try {
			assertPending();
			this.state = State.SUCCESS;
			this.value = value;
			if (hasBlockers) {
				monitor.notifyAll();
			}
		} finally {
			lock.unlock();
		}
		getObservable().notify(complete.getT2(), Event.wrap(this));

	}

	@Override
	protected void doResolution() {
		if (null != supplier) {
			Functions.schedule(
					new Consumer<Void>() {
						@Override
						public void accept(Void v) {
							try {
								notifyValue(supplier.get());
							} catch (Throwable t) {
								notifyError(t);
							}
						}
					},
					null,
					getObservable()
			);
		}
	}

	private void assertPending() {
		Assert.state(state == State.PENDING, "Promise has already completed. ");
	}

	private enum State {
		PENDING, SUCCESS, FAILURE
	}

	@Override
	public String toString() {
		return "Promise{" +
				"value=" + value +
				", state=" + state +
				", error=" + error +
				'}';
	}

}