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

package reactor.rx;

import org.reactivestreams.spi.Subscriber;
import reactor.core.Environment;
import reactor.core.Observable;
import reactor.event.dispatch.Dispatcher;
import reactor.event.lifecycle.Pausable;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Predicate;
import reactor.function.Supplier;
import reactor.rx.action.*;
import reactor.timer.Timer;

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
 * promise also provides methods for composing actions with the future value much like a {@link reactor.rx.Stream}.
 * However, where
 * a {@link reactor.rx.Stream} can process many values, a {@code Promise} processes only one value or error.
 * <p/>
 * Reactor's {@code Promise} implementation is modeled largely after the <a href="https://github
 * .com/promises-aplus/promises-spec">Promises/A+
 * specification</a>, which defines a number of methods and potential actions for promises.
 *
 * @param <O> the type of the value that will be made available
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @see <a href="https://github.com/promises-aplus/promises-spec">Promises/A+ specification</a>
 */
public class Promise<O> implements Pipeline<O>, Supplier<O> {

	private final ReentrantLock lock = new ReentrantLock();

	private final long         defaultTimeout;
	private final Condition    pendingCondition;
	final Action<?, O> delegateAction;

	private State state = State.PENDING;
	private O         value;
	private Throwable error;
	private boolean hasBlockers = false;

	/**
	 * Creates a new unfulfilled promise.
	 * <p/>
	 * The {@code dispatcher} is used when notifying the Promise's consumers, determining the thread on which they are
	 * called. The given {@code env} is used to determine the default await timeout. If {@code env} is {@code null} the
	 * default await timeout will be 30 seconds. This Promise will consumer errors from its {@code parent} such that if
	 * the parent completes in error then so too will this Promise.
	 *
	 * @param delegateAction The Action to use to call Consumers
	 * @param env        The Environment, if any, from which the default await timeout is obtained
	 */
	public Promise(Action<?,O> delegateAction,
	               @Nullable Environment env) {
		this.delegateAction = delegateAction;
		delegateAction.env(env).prefetch(1);
		this.defaultTimeout = env != null ? env.getProperty("reactor.await.defaultTimeout", Long.class, 30000L) : 30000L;
		this.pendingCondition = lock.newCondition();
	}

	/**
	 * Creates a new promise that has been fulfilled with the given {@code value}.
	 * <p/>
	 * The {@code observable} is used when notifying the Promise's consumers. The given {@code env} is used to determine
	 * the default await timeout. If {@code env} is {@code null} the default await timeout will be 30 seconds.
	 *
	 * @param value      The value that fulfills the promise
	 * @param delegateAction The Action to use to call Consumers
	 * @param env        The Environment, if any, from which the default await timeout is obtained
	 */
	public Promise(O value,
	               Action<?, O> delegateAction,
	               @Nullable Environment env) {
		this(delegateAction, env);
		this.value = value;
		this.state = State.SUCCESS;
		delegateAction.setState(Stream.State.COMPLETE);
	}

	/**
	 * Creates a new promise that has failed with the given {@code error}.
	 * <p/>
	 * The {@code observable} is used when notifying the Promise's consumers, determining the thread on which they are
	 * called. The given {@code env} is used to determine the default await timeout. If {@code env} is {@code null} the
	 * default await timeout will be 30 seconds.
	 *
	 * @param error      The error the completed the promise
	 * @param delegateAction The Action to use to call Consumers
	 * @param env        The Environment, if any, from which the default await timeout is obtained
	 */
	public Promise(Throwable error,
	               Action<?, O> delegateAction,
	               @Nullable Environment env) {
		this(delegateAction, env);
		this.error = error;
		this.state = State.FAILURE;
		delegateAction.setState(Stream.State.ERROR);
		delegateAction.error = error;
	}

	/**
	 * Assign a {@link Consumer} that will either be invoked later, when the {@code Promise} is completed by either
	 * setting a value or propagating an error, or, if this {@code Promise} has already been fulfilled, is immediately
	 * scheduled to be executed on the current {@link reactor.event.dispatch.Dispatcher}.
	 *
	 * @param onComplete the completion {@link Consumer}
	 * @return {@literal this}
	 */
	public Promise<O> onComplete(@Nonnull final Consumer<Promise<O>> onComplete) {
		connect(new CompleteAction<O, Promise<O>>(delegateAction.getDispatcher(), this, onComplete));
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
	public Promise<O> onSuccess(@Nonnull final Consumer<O> onSuccess) {
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
	public Promise<O> onError(@Nullable final Consumer<Throwable> onError) {
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
	public Promise<O> then(@Nonnull Consumer<O> onSuccess, @Nullable Consumer<Throwable> onError) {
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
	public <V> Promise<V> then(@Nonnull final Function<O, V> onSuccess, @Nullable final Consumer<Throwable> onError) {
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
	 * Reactor's {@link Environment} properties using the key {@code reactor.await.defaultTimeout} is used. The
	 * default is
	 * 30 seconds. If the promise is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has
	 * not
	 * completed
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 * @throws RuntimeException     if the promise is completed with an error
	 */
	public O await() throws InterruptedException {
		return await(defaultTimeout, TimeUnit.MILLISECONDS);
	}

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this {@code Promise}. If the
	 * promise
	 * is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @param timeout the timeout value
	 * @param unit    the {@link TimeUnit} of the timeout value
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has
	 * not
	 * completed
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 */
	public O await(long timeout, TimeUnit unit) throws InterruptedException {
		if (isPending()) {
			broadcastFlush();
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
	public O get() {
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
	 * Return the error (if any) that has completed this {@code Promise}. Returns {@code null} if the promise has not
	 * been
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
	public <E> Promise<E> connect(@Nonnull final Action<O, E> action) {
		Promise<E> promise = new Promise<E>(action, delegateAction.getEnvironment());
		produceTo(action);
		return promise;
	}

	public Promise<O> consume(@Nonnull Consumer<O> consumer) {
		connect(new CallbackAction<O>(delegateAction.getDispatcher(), consumer));
		return this;
	}

	public Promise<O> consume(@Nonnull Object key, @Nonnull Observable observable) {
		connect(new ObservableAction<O>(delegateAction.getDispatcher(), observable, key));
		return this;
	}

	public <V, C extends Stream<V>> Promise<V> fork(@Nonnull Function<O, C> fn) {
		final MapManyAction<O, V, C> d = new MapManyAction<O, V, C>(fn, delegateAction.getDispatcher());
		return connect(d);
	}

	public <E extends Throwable> Promise<O> when(@Nonnull Class<E> exceptionType, @Nonnull Consumer<E> onError) {
		connect(new ErrorAction<O, E>(delegateAction.getDispatcher(), Selectors.T(exceptionType), onError));
		return this;
	}

	public <E extends Throwable> Promise<E> recover(@Nonnull Class<E> exceptionType) {
		RecoverAction<O, E> recoverAction = new RecoverAction<O, E>(delegateAction.getDispatcher(),
				Selectors.T(exceptionType));
		return connect(recoverAction);
	}

	public <V> Promise<V> map(@Nonnull final Function<O, V> fn) {
		final MapAction<O, V> d = new MapAction<O, V>(fn, delegateAction.getDispatcher());
		return connect(d);
	}

	public FilterAction<O, Promise<O>> filter(@Nonnull final Predicate<O> p) {
		final FilterAction<O, Promise<O>> d = new FilterAction<O, Promise<O>>(p, delegateAction.getDispatcher());
		connect(d);
		return d;
	}

	@SuppressWarnings("unchecked")
	public FilterAction<Boolean, Promise<Boolean>> filter() {
		return ((Promise<Boolean>) this).filter(FilterAction.simplePredicate);
	}

	public FilterAction<O, Promise<O>> filter(@Nonnull Function<O, Boolean> fn) {
		final FilterAction<O, Promise<O>> d = new FilterAction<O, Promise<O>>(fn, delegateAction.getDispatcher());
		connect(d);
		return d;
	}

	public Promise<O> merge(Promise<O>... composables) {
		return connect(new MergeAction<O>(delegateAction.getDispatcher(), composables));
	}

	public Promise<O> timeout(long timeout) {
		delegateAction.timeout(timeout);
		return this;
	}

	public Promise<O> timeout(long timeout, Timer timer) {
		delegateAction.timeout(timeout, timer);
		return this;
	}

	public <E> Promise<E> propagate(Supplier<E> supplier) {
		return connect(new SupplierAction<O,E>(delegateAction.getDispatcher(), supplier));
	}

	@Override
	public void broadcastError(Throwable ev) {
		errorAccepted(ev);
		delegateAction.broadcastError(ev);
	}

	@Override
	public void broadcastComplete() {
		delegateAction.broadcastComplete();
	}

	@Override
	public void broadcastNext(O ev) {
		valueAccepted(ev);
		delegateAction.broadcastNext(ev);
	}

	@Override
	public void broadcastFlush() {
		delegateAction.broadcastFlush();
	}

	@Override
	public Pausable cancel() {
		delegateAction.cancel();
		return this;
	}

	@Override
	public Pausable pause() {
		delegateAction.pause();
		return this;
	}

	@Override
	public Pausable resume() {
		delegateAction.resume();
		return this;
	}

	@Override
	public Promise<O> getPublisher() {
		return this;
	}

	@Override
	public void produceTo(org.reactivestreams.api.Consumer<O> consumer) {
		lock.lock();
		try {
			if (!isComplete()) {
				delegateAction.produceTo(consumer);
			}else{
				if(isError()){
					consumer.getSubscriber().onError(error);
				}else if(isSuccess()){
					consumer.getSubscriber().onNext(value);
				}
				consumer.getSubscriber().onComplete();
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void subscribe(Subscriber<O> subscriber) {
		delegateAction.subscribe(subscriber);
	}

	public Environment getEnvironment() {
		return delegateAction.getEnvironment();
	}


	protected void errorAccepted(Throwable error) {
		lock.lock();
		try {
			if (!isPending()) return;
			this.state = State.FAILURE;
			this.error = error;
			if (hasBlockers) {
				pendingCondition.signalAll();
				hasBlockers = false;
			}
		} finally {
			lock.unlock();
		}
	}

	protected void valueAccepted(O value) {
		lock.lock();
		try {
			if (!isPending()) return;
			this.state = State.SUCCESS;
			this.value = value;
			if (hasBlockers) {
				pendingCondition.signalAll();
				hasBlockers = false;
			}
		} finally {
			lock.unlock();
		}
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