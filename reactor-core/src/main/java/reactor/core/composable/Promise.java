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
import reactor.core.Observable;
import reactor.core.action.Action;
import reactor.core.action.CallbackAction;
import reactor.core.action.ConnectAction;
import reactor.core.spec.Reactors;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Predicate;
import reactor.function.Supplier;
import reactor.timer.Timer;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@code Promise} is a stateful event processor that accepts a single value or error. In addition to {@link #get()
 * getting} or {@link #await() awaiting} the value, consumers can be registered to be notified of {@link
 * #onError(Consumer) notified an error}, {@link #onSuccess(Consumer) a value}, or {@link #onComplete(Consumer) both}.
 * A
 * promise also provides methods for composing actions with the future value much like a {@link Stream}. However, where
 * a {@link Stream} can process many values, a {@code Promise} processes only one value or error.
 * <p/>
 * Reactor's {@code Promise} implementation is modeled largely after the <a href="https://github.com/promises-aplus/promises-spec">Promises/A+
 * specification</a>, which defines a number of methods and potential actions for promises.
 *
 * @param <T> the type of the value that will be made available
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 * @see <a href="https://github.com/promises-aplus/promises-spec">Promises/A+ specification</a>
 */
public class Promise<T> extends Composable<T> implements Supplier<T> {

	private final Selector      complete = Selectors.anonymous();
	private final ReentrantLock lock     = new ReentrantLock();

	private final long        defaultTimeout;
	private final Condition   pendingCondition;

	private State state = State.PENDING;
	private T           value;
	private Throwable   error;
	private Supplier<T> supplier;
	private boolean hasBlockers = false;

	/**
	 * Creates a new unfulfilled promise.
	 * <p/>
	 * The {@code observable} is used when notifying the Promise's consumers, determining the thread on which they are
	 * called. The given {@code env} is used to determine the default await timeout. If {@code env} is {@code null} the
	 * default await timeout will be 30 seconds. This Promise will consumer errors from its {@code parent} such that if
	 * the parent completes in error then so too will this Promise.
	 *
	 * @param observable The Observable to use to call Consumers
	 * @param env        The Environment, if any, from which the default await timeout is obtained
	 * @param parent     The parent, if any, from which errors are consumed
	 */
	public Promise(@Nullable Observable observable,
	               @Nullable Environment env,
	               @Nullable Composable<?> parent) {
		super(observable, parent, null, env);
		this.defaultTimeout = env != null ? env.getProperty("reactor.await.defaultTimeout", Long.class, 30000L) : 30000L;
		this.pendingCondition = lock.newCondition();

		consumeEvent(new Consumer<Event<T>>() {
			@Override
			public void accept(Event<T> event) {
				valueAccepted(event.getData());
			}
		});
		when(Throwable.class, new Consumer<Throwable>() {
			@Override
			public void accept(Throwable throwable) {
				errorAccepted(throwable);
			}
		});
	}

	/**
	 * Creates a new promise that has been fulfilled with the given {@code value}.
	 * <p/>
	 * The {@code observable} is used when notifying the Promise's consumers. The given {@code env} is used to determine
	 * the default await timeout. If {@code env} is {@code null} the default await timeout will be 30 seconds.
	 *
	 * @param value      The value that fulfills the promise
	 * @param observable The Observable to use to call Consumers
	 * @param env        The Environment, if any, from which the default await timeout is obtained
	 */
	public Promise(T value,
	               @Nullable Observable observable,
	               @Nullable Environment env) {
		this(observable, env, null);
		this.value = value;
		this.state = State.SUCCESS;
	}

	/**
	 * Creates a new promise that will be fulfilled with the value obtained from the given {@code valueSupplier}.
	 * <p/>
	 * The {@code observable} is used when notifying the Promise's consumers, determining the thread on which they are
	 * called. The given {@code env} is used to determine the default await timeout. If {@code env} is {@code null} the
	 * default await timeout will be 30 seconds.
	 *
	 * @param valueSupplier The Supplier of the value that fulfills the promise
	 * @param observable    The Observable to use to call Consumers
	 * @param env           The Environment, if any, from which the default await timeout is obtained
	 */
	public Promise(Supplier<T> valueSupplier,
	               @Nonnull Observable observable,
	               @Nullable Environment env) {
		this(observable, env, null);
		this.supplier = valueSupplier;
	}

	/**
	 * Creates a new promise that has failed with the given {@code error}.
	 * <p/>
	 * The {@code observable} is used when notifying the Promise's consumers, determining the thread on which they are
	 * called. The given {@code env} is used to determine the default await timeout. If {@code env} is {@code null} the
	 * default await timeout will be 30 seconds.
	 *
	 * @param error      The error the completed the promise
	 * @param env        The Environment, if any, from which the default await timeout is obtained
	 * @param observable The Observable to use to call Consumers
	 */
	public Promise(Throwable error,
	               @Nonnull Observable observable,
	               @Nullable Environment env) {
		this(observable, env, null);
		this.error = error;
		this.state = State.FAILURE;
	}

	/**
	 * Assign a {@link Consumer} that will either be invoked later, when the {@code Promise} is completed by either
	 * setting a value or propagating an error, or, if this {@code Promise} has already been fulfilled, is immediately
	 * scheduled to be executed on the current {@link reactor.event.dispatch.Dispatcher}.
	 *
	 * @param onComplete the completion {@link Consumer}
	 * @return {@literal this}
	 */
	public Promise<T> onComplete(@Nonnull final Consumer<Promise<T>> onComplete) {
		if (isComplete()) {
			Reactors.schedule(onComplete, this, getObservable());
		} else {
			getObservable().on(complete, new CallbackAction<Promise<T>>(onComplete, getObservable(), null));
		}
		return this;
	}

	/**
	 * Assign a {@link Consumer} that will either be invoked later, when the {@code Promise} is successfully completed
	 * with
	 * a value, or, if this {@code Promise} has already been fulfilled, is immediately scheduled to be executed on the
	 * current {@link Dispatcher}.
	 *
	 * @param onSuccess the success {@link Consumer}
	 * @return {@literal this}
	 */
	public Promise<T> onSuccess(@Nonnull final Consumer<T> onSuccess) {
		return consume(onSuccess);
	}

	/**
	 * Assign a {@link Consumer} that will either be invoked later, when the {@code Promise} is completed with an error,
	 * or, if this {@code Promise} has already been fulfilled, is immediately scheduled to be executed on the current
	 * {@link Dispatcher}.
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
	 * Assign both a success {@link Consumer} and an optional (possibly {@code null}) error {@link Consumer}.
	 *
	 * @param onSuccess the success {@link Consumer}
	 * @param onError   the error {@link Consumer}
	 * @return {@literal this}
	 * @see #onSuccess(Consumer)
	 * @see #onError(Consumer)
	 */
	public Promise<T> then(@Nonnull Consumer<T> onSuccess, @Nullable Consumer<Throwable> onError) {
		return onSuccess(onSuccess).onError(onError);
	}

	/**
	 * Assign a success {@link Function} that will either be invoked later, when the {@code Promise} is successfully
	 * completed with a value, or, if this {@code Promise} has already been fulfilled, the function is immediately
	 * scheduled to be executed on the current {@link reactor.event.dispatch.Dispatcher}.
	 * <p/>
	 * A new {@code Promise} is returned that will be populated by result of the given transformation {@link Function}
	 * that
	 * turns the incoming {@code T} into a {@code V}.
	 *
	 * @param onSuccess the success transformation {@link Function}
	 * @param onError   the error {@link Consumer}
	 * @param <V>       the type of the value returned by the transformation {@link Function}
	 * @return a new {@code Promise} that will be populated by the result of the transformation {@link Function}
	 */
	public <V> Promise<V> then(@Nonnull final Function<T, V> onSuccess, @Nullable final Consumer<Throwable> onError) {
		onError(onError);
		return map(onSuccess);
	}

	/**
	 * Indicates whether this {@code Promise} has been completed with either an error or a value
	 *
	 * @return {@code true} if this {@code Promise} is complete, {@code false} otherwise.
	 * @see #isPending()
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
	 * Indicates whether this {@code Promise} has yet to be completed with a value or an error.
	 *
	 * @return {@code true} if this {@code Promise} is still pending, {@code false} otherwise.
	 * @see #isComplete()
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
	 * Indicates whether this {@code Promise} has been successfully completed a value.
	 *
	 * @return {@code true} if this {@code Promise} is successful, {@code false} otherwise.
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
	 * Indicates whether this {@code Promise} has been completed with an error.
	 *
	 * @return {@code true} if this {@code Promise} was completed with an error, {@code false} otherwise.
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
	 * 30 seconds. If the promise is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has not
	 * completed
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 * @throws RuntimeException     if the promise is completed with an error
	 */
	public T await() throws InterruptedException {
		return await(defaultTimeout, TimeUnit.MILLISECONDS);
	}

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this {@code Promise}. If the promise
	 * is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @param timeout the timeout value
	 * @param unit    the {@link TimeUnit} of the timeout value
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has not
	 * completed
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 */
	public T await(long timeout, TimeUnit unit) throws InterruptedException {
		if (isPending()) {
			flush();
		}

		if (!isPending()) {
			return get();
		}

		lock.lock();
		try {
			hasBlockers = true;
			if (timeout >= 0) {
				long msTimeout = TimeUnit.MILLISECONDS.convert(timeout, unit);
				long endTime = System.currentTimeMillis() + msTimeout;
				while (state == State.PENDING && (System.currentTimeMillis()) < endTime) {
					this.pendingCondition.await(200, TimeUnit.MILLISECONDS);
				}
			} else {
				while (state == State.PENDING) {
					this.pendingCondition.await(200, TimeUnit.MILLISECONDS);
				}
			}
		} finally {
			hasBlockers = false;
			lock.unlock();
		}

		return get();
	}

	/**
	 * Returns the value that completed this promise. Returns {@code null} if the promise has not been completed. If the
	 * promise is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value that completed the promise, or {@code null} if it has not been completed
	 * @throws RuntimeException if the promise was completed with an error
	 */
	@Override
	public T get() {
		if (isPending()) {
			flush();
		}
		lock.lock();
		try {
			if (state == State.SUCCESS) {
				return value;
			} else if (state == State.FAILURE) {
				if (RuntimeException.class.isInstance(error)) {
					throw (RuntimeException) error;
				} else {
					throw new RuntimeException(error);
				}
			} else {
				return null;
			}
		} finally {
			lock.unlock();
		}
	}

	/**
	 * Return the error (if any) that has completed this {@code Promise}. Returns {@code null} if the promise has not been
	 * completed, or was completed with a value.
	 *
	 * @return the error (if any)
	 */
	public Throwable reason() {
		lock.lock();
		try {
			return error;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Promise<T> consume(@Nonnull Consumer<T> consumer) {
		return (Promise<T>) super.consume(consumer);
	}

	@Override
	public Promise<T> connect(@Nonnull final Composable<T> composable) {
		return (Promise<T>) super.connect(composable);
	}

	@Override
	public Promise<T> consume(@Nonnull Object key, @Nonnull Observable observable) {
		return (Promise<T>) super.consume(key, observable);
	}

	@Override
	public <V, C extends Composable<V>> Promise<V> mapMany(@Nonnull Function<T, C> fn) {
		return (Promise<V>) super.mapMany(fn);
	}

	@Override
	@SuppressWarnings("unchecked")
	public <E extends Throwable> Promise<T> when(@Nonnull Class<E> exceptionType, @Nonnull Consumer<E> onError) {
		lock.lock();
		try {
			if (state == State.FAILURE) {
				Reactors.schedule(
						new CallbackAction<E>(onError, getObservable(), null),
						Event.wrap((E) error), getObservable());
			} else {
				super.when(exceptionType, onError);
			}
			return this;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public <V> Promise<V> map(@Nonnull final Function<T, V> fn) {
		return (Promise<V>) super.map(fn);
	}

	@Override
	public Promise<T> filter(@Nonnull final Predicate<T> p) {
		return (Promise<T>) super.filter(p);
	}

	@Override
	public Promise<T> filter(@Nonnull final Predicate<T> p, final Composable<T> elseComposable) {
		return (Promise<T>) super.filter(p, elseComposable);
	}

	@Override
	public Promise<Boolean> filter() {
		return (Promise<Boolean>) super.filter();
	}

	@Override
	public Promise<Boolean> filter(@Nonnull Composable<Boolean> elseComposable) {
		return (Promise<Boolean>) super.filter(elseComposable);
	}

	@Override
	public Promise<T> filter(@Nonnull Function<T, Boolean> fn) {
		return (Promise<T>)super.filter(fn);
	}

	@Override
	public Promise<T> timeout(long timeout) {
		return (Promise<T>)super.timeout(timeout);
	}

	@Override
	public Promise<T> timeout(long timeout, Timer timer) {
		return (Promise<T>)super.timeout(timeout, timer);
	}

	@Override
	public Promise<T> flush() {
		return (Promise<T>) super.flush();
	}

	@Override
	public Promise<T> add(Action<T> operation) {
		lock.lock();
		try {
			if (state == State.SUCCESS) {
				Reactors.schedule(operation, Event.wrap(value), getObservable());
			} else if (state == State.FAILURE) {
				Reactors.schedule(
						new ConnectAction<Throwable>(operation.getObservable(), operation.getFailureKey(), null),
						Event.wrap(error), getObservable());
			} else {
				super.add(operation);
			}
			return this;
		} finally {
			lock.unlock();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	protected <V, C extends Composable<V>> Deferred<V, C> createDeferred() {
		return (Deferred<V, C>) new Deferred<V, Promise<V>>(new Promise<V>(null, getEnvironment(), this));
	}

	protected void errorAccepted(Throwable error) {
		lock.lock();
		try {
			assertPending();
			this.error = error;
			this.state = State.FAILURE;
			if (hasBlockers) {
				pendingCondition.signalAll();
				hasBlockers = false;
			}
		} finally {
			lock.unlock();
		}
		getObservable().notify(complete.getObject(), Event.wrap(this));

	}

	protected void valueAccepted(T value) {
		lock.lock();
		try {
			assertPending();
			this.value = value;
			this.state = State.SUCCESS;
			if (hasBlockers) {
				pendingCondition.signalAll();
				hasBlockers = false;
			}
		} finally {
			lock.unlock();
		}
		getObservable().notify(complete.getObject(), Event.wrap(this));
	}

	@Override
	void notifyValue(Event<T> value) {
		lock.lock();
		try {
			assertPending();
		} finally {
			lock.unlock();
		}
		super.notifyValue(value);
	}

	@Override
	void notifyError(Throwable error) {
		lock.lock();
		try {
			assertPending();
		} finally {
			lock.unlock();
		}
		super.notifyError(error);
	}

	@Override
	void notifyFlush() {
		if (null != supplier) {
			lock.lock();
			try {
				assertPending();
			} finally {
				lock.unlock();
			}


			final Consumer<Event<T>> consumer = getObservable().prepare(getAcceptKey());
			Reactors.schedule(new Consumer<Void>() {
				@Override
				public void accept(Void v) {
					try {
						consumer.accept(Event.wrap(supplier.get()));
					} catch (Throwable t) {
						notifyError(t);
					}
				}

			}, null, getObservable());


		}
	}

	private void assertPending() {
		Assert.state(isPending(), "Promise has already completed. ");
	}

	private enum State {
		PENDING, SUCCESS, FAILURE;
	}

	@Override
	public String toString() {
		lock.lock();
		try {
			return "Promise{" +
					"value=" + value +
					", error=" + error +
					'}';
		} finally {
			lock.unlock();
		}
	}

}