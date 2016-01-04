/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.rx;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Flux;
import reactor.Mono;
import reactor.Timers;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.core.timer.Timer;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.rx.broadcast.Broadcaster;

/**
 * A {@code Promise} is a stateful event container that accepts a single value or error. In addition to {@link #get()
 * getting} or {@link #await() awaiting} the value, consumers can be registered to the outbound {@link #stream()} or via
 * , consumers can be registered to be notified of {@link #doOnError(Consumer) notified an error}, {@link
 * #doOnSuccess(Consumer) a value}, or {@link #onComplete(Consumer) both}. <p> A promise also provides methods for
 * composing actions with the future value much like a {@link reactor.rx.Stream}. However, where a {@link
 * reactor.rx.Stream} can process many values, a {@code Promise} processes only one value or error.
 *
 * @param <O> the type of the value that will be made available
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @see <a href="https://github.com/promises-aplus/promises-spec">Promises/A+ specification</a>
 */
public final class Promise<O> extends Mono<O>
		implements Supplier<O>, Processor<O, O>, Consumer<O>, ReactiveState.Bounded, Subscription,
		           ReactiveState.Upstream, ReactiveState.Downstream, ReactiveState.ActiveUpstream {

	private final static Promise                            COMPLETE  = success(null);
	private static final AtomicIntegerFieldUpdater<Promise> REQUESTED =
			AtomicIntegerFieldUpdater.newUpdater(Promise.class, "requested");

	/**
	 * Create synchronous {@link Promise} and use the given error to complete the {@link Promise} immediately.
	 *
	 * @param error the error to complete the {@link Promise} with
	 * @param <T> the type of the value
	 *
	 * @return A {@link Promise} that is completed with the given error
	 */
	public static <T> Promise<T> error(Throwable error) {
		return error(Timers.globalOrNull(), error);
	}

	/**
	 * Create a {@link Promise} and use the given error to complete the {@link Promise} immediately.
	 *
	 * @param error the error to complete the {@link Promise} with
	 * @param timer the {@link Timer} to use by default for scheduled operations
	 * @param <T> the type of the value
	 *
	 * @return A {@link Promise} that is completed with the given error
	 */
	public static <T> Promise<T> error(Timer timer, Throwable error) {
		return new Promise<T>(error, timer);
	}


	/**
	 * Create a synchronous {@link Promise}.
	 *
	 * @param <T> type of the expected value
	 *
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> prepare() {
		return prepare(Timers.globalOrNull());
	}

	/**
	 * Create a `{@link Promise}.
	 *
	 * @param timer the {@link Timer} to use by default for scheduled operations
	 * @param <T> type of the expected value
	 *
	 * @return a new {@link Promise}
	 */
	public static <T> Promise<T> prepare(Timer timer) {
		Promise<T> p = new Promise<T>(timer);
		p.request(1);
		return p;
	}

	/**
	 * Create a {@link Promise}.
	 *
	 * @param timer the {@link Timer} to use by default for scheduled operations
	 * @param <T> type of the expected value
	 *
	 * @return a new {@link Promise}
	 */
	public static <T> Promise<T> ready(Timer timer) {
		return new Promise<T>(timer);
	}

	/**
	 * Create a synchronous {@link Promise}.
	 *
	 * @param <T> type of the expected value
	 * @return A {@link Promise}.
	 */
	public static <T> Promise<T> ready() {
		return ready(Timers.globalOrNull());
	}

	/**
	 * Create a {@link Promise} already completed without any data.
	 *
	 * @return A {@link Promise} that is completed
	 */
	@SuppressWarnings("unchecked")
	public static Promise<Void> success() {
		return COMPLETE;
	}

	/**
	 * Create a {@link Promise} using the given value to complete the {@link Promise} immediately.
	 *
	 * @param value the value to complete the {@link Promise} with
	 * @param <T> the type of the value
	 *
	 * @return A {@link Promise} that is completed with the given value
	 */
	public static <T> Promise<T> success(T value) {
		return success(Timers.globalOrNull(), value);
	}

	/**
	 * Create a {@link Promise} using the given value to complete the {@link Promise} immediately.
	 *
	 * @param value the value to complete the {@link Promise} with
	 * @param timer the {@link Timer} to use by default for scheduled operations
	 * @param <T> the type of the value
	 *
	 * @return A {@link Promise} that is completed with the given value
	 */
	public static <T> Promise<T> success(Timer timer, T value) {
		return new Promise<T>(value, timer);
	}

	private final ReentrantLock lock = new ReentrantLock();
	private final long      defaultTimeout;
	private final Condition pendingCondition;
	private final Timer     timer;

	protected Subscription subscription;

	Broadcaster<O> outboundStream;
	FinalState finalState = null;

	private volatile int requested = 0;
	private O value;

	private Throwable error;

	private boolean hasBlockers = false;

	/**
	 * Creates a new unfulfilled promise. <p> The {@code dispatcher} is used when notifying the Promise's consumers,
	 * determining the thread on which they are called. The given {@code env} is used to determine the default await
	 * timeout. The default await timeout will be 30 seconds.
	 */
	Promise() {
		this(null);
	}

	/**
	 * Creates a new unfulfilled promise. <p> The {@code dispatcher} is used when notifying the Promise's consumers,
	 * determining the thread on which they are called. The given {@code env} is used to determine the default await
	 * timeout.
	 *
	 * @param timer The default Timer for time-sensitive downstream actions if any.
	 */
	Promise(@Nullable Timer timer) {
		this.timer = timer;
		this.defaultTimeout = DEFAULT_TIMEOUT;
		this.pendingCondition = lock.newCondition();
	}

	/**
	 * Creates a new promise that has been fulfilled with the given {@code value}.
	 *
	 * @param value The value that fulfills the promise
	 * @param timer The default Timer for time-sensitive downstream actions if any.
	 */
	Promise(O value, @Nullable Timer timer) {
		this(timer);
		finalState = FinalState.COMPLETE;
		this.value = value;
	}

	/**
	 * Creates a new promise that has failed with the given {@code error}. <p> The {@code observable} is used when
	 * notifying the Promise's consumers, determining the thread on which they are called.
	 * If {@code env} is {@code null} the default await timeout will be 30
	 * seconds.
	 *
	 * @param error The error the completed the promise
	 * @param timer The default Timer for time-sensitive downstream actions if any.
	 */
	Promise(Throwable error, @Nullable Timer timer) {
		this(timer);
		finalState = FinalState.ERROR;
		this.error = error;
	}

	/**
	 * Creates a new promise that has the given state. <p> The {@code observable} is used
	 * when
	 * notifying the Promise's consumers, determining the thread on which they are called. The given {@code env} is used
	 * to determine the default await timeout.
	 *
	 * @param state The state of the promise
	 * @param timer The default Timer for time-sensitive downstream actions if any.
	 */
	Promise(FinalState state, @Nullable Timer timer) {
		this(timer);
		this.finalState = state;
	}

	@Override
	public void accept(O o) {
		valueAccepted(o);
	}

	/**
	 * Block the calling thread, waiting for the completion of this {@code Promise}. A default timeout as specified in
	 * {@link System#getProperties()} using the key {@code reactor.await.defaultTimeout} is used. The default is 30
	 * seconds. If the promise is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has
	 * not completed
	 *
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 * @throws RuntimeException     if the promise is completed with an error
	 */
	public O await() throws InterruptedException {
		return await(defaultTimeout, TimeUnit.MILLISECONDS);
	}

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this {@code Promise}.
	 *
	 * @param timeout the timeout value
	 * @param unit the {@link TimeUnit} of the timeout value
	 *
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has
	 * not completed
	 *
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 */
	public O await(long timeout, TimeUnit unit) throws InterruptedException {
		request(1);
		if (!isPending()) {
			return get();
		}

		lock.lock();
		try {
			hasBlockers = true;
			if (timeout >= 0) {
				long msTimeout = TimeUnit.MILLISECONDS.convert(timeout, unit);
				long endTime = System.currentTimeMillis() + msTimeout;
				while (finalState == null && (System.currentTimeMillis()) < endTime) {
					this.pendingCondition.await(200, TimeUnit.MILLISECONDS);
				}
			}
			else {
				while (finalState == null) {
					this.pendingCondition.await(200, TimeUnit.MILLISECONDS);
				}
			}
		}
		finally {
			hasBlockers = false;
			lock.unlock();
		}

		return get();
	}

	/**
	 * Block the calling thread, waiting for the completion of this {@code Promise}. A default timeout as specified in
	 * {@link System#getProperties()} using the key {@code reactor.await.defaultTimeout} is used. The default is 30
	 * seconds. If the promise is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return true if complete without error
	 *
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 * @throws RuntimeException     if the promise is completed with an error
	 */
	public boolean awaitSuccess() throws InterruptedException {
		return awaitSuccess(defaultTimeout, TimeUnit.MILLISECONDS);
	}

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this {@code Promise}.
	 *
	 * @param timeout the timeout value
	 * @param unit the {@link TimeUnit} of the timeout value
	 *
	 * @return true if complete without error completed
	 *
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 */
	public boolean awaitSuccess(long timeout, TimeUnit unit) throws InterruptedException {
		await(timeout, unit);
		return isSuccess();
	}

	@Override
	public void cancel() {
		Subscription subscription = this.subscription;
		if (subscription != null) {
			this.subscription = null;
			subscription.cancel();
		}
	}

	protected void completeAccepted() {
		lock.lock();
		try {
			if (isPending()) {
				valueAccepted(null);
			}

			subscription = null;
		}
		finally {
			lock.unlock();
		}
	}

	public ReactiveStateUtils.Graph debug() {
		return ReactiveStateUtils.scan(this);
	}

	@Override
	public Subscriber downstream() {
		return outboundStream;
	}

	protected void errorAccepted(Throwable error) {
		lock.lock();
		try {
			if (!isPending()) {
				throw ReactorFatalException.create(error);
			}

			this.error = error;
			this.finalState = FinalState.ERROR;

			subscription = null;

			if (outboundStream != null) {
				outboundStream.onError(error);
			}

			if (hasBlockers) {
				pendingCondition.signalAll();
				hasBlockers = false;
			}
		}
		finally {
			lock.unlock();
		}
	}

	/**
	 * Returns the value that completed this promise. Returns {@code null} if the promise has not been completed. If the
	 * promise is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value that completed the promise, or {@code null} if it has not been completed
	 *
	 * @throws RuntimeException if the promise was completed with an error
	 */
	@Override
	public O get() {
		request(1);
		lock.lock();
		try {
			if (finalState == FinalState.COMPLETE) {
				return value;
			}
			else if (finalState == FinalState.ERROR) {
				if (RuntimeException.class.isInstance(error)) {
					throw (RuntimeException) error;
				}
				else {
					throw new RuntimeException(error);
				}
			}
			else {
				return null;
			}
		}
		finally {
			lock.unlock();
		}
	}

	public Timer getTimer() {
		return timer;
	}

	/**
	 * Indicates whether this {@code Promise} has been completed with either an error or a value
	 *
	 * @return {@code true} if this {@code Promise} is complete, {@code false} otherwise.
	 *
	 * @see #isPending()
	 */

	public boolean isComplete() {
		lock.lock();
		try {
			return finalState != null;
		}
		finally {
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
			return finalState == FinalState.ERROR;
		}
		finally {
			lock.unlock();
		}
	}

	/**
	 * Indicates whether this {@code Promise} has yet to be completed with a value or an error.
	 *
	 * @return {@code true} if this {@code Promise} is still pending, {@code false} otherwise.
	 *
	 * @see #isComplete()
	 */
	public boolean isPending() {
		lock.lock();
		try {
			return finalState == null;
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public boolean isStarted() {
		return requested > 0;
	}

	/**
	 * Indicates whether this {@code Promise} has been successfully completed a value.
	 *
	 * @return {@code true} if this {@code Promise} is successful, {@code false} otherwise.
	 */
	public boolean isSuccess() {
		lock.lock();
		try {
			return finalState == FinalState.COMPLETE;
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public boolean isTerminated() {
		return finalState == FinalState.COMPLETE;
	}


	/**
	 * Assign a {@link Consumer} that will either be invoked later, when the {@code Promise} is completed by either
	 * setting a value or propagating an error, or, if this {@code Promise} has already been fulfilled, is immediately
	 * scheduled to be executed.
	 *
	 * @param onComplete the completion {@link Consumer}
	 *
	 * @return {@literal the new Promise}
	 */
	public final Promise<O> onComplete(@Nonnull final Consumer<Promise<O>> onComplete) {
		request(1);
		lock.lock();
		try {
			if (finalState == FinalState.ERROR) {
				onComplete.accept(this);
				return new Promise<>(error, timer);
			}
			else if (finalState == FinalState.COMPLETE) {
				onComplete.accept(this);
				return new Promise<>(value, timer);
			}
		}
		catch (Throwable t) {
			return new Promise<>(t, timer);
		}
		finally {
			lock.unlock();
		}

		return stream().lift(new Flux.Operator<O, O>() {
			@Override
			public Subscriber<? super O> apply(final Subscriber<? super O> subscriber) {
				return new SubscriberBarrier<O, O>(subscriber) {
					@Override
					protected void doComplete() {
						onComplete.accept(Promise.this);
						subscriber.onComplete();
					}

					@Override
					protected void doError(Throwable throwable) {
						onComplete.accept(Promise.this);
						subscriber.onError(throwable);
					}

					@Override
					protected void doNext(O o) {
						doCancel();
						onComplete.accept(Promise.this);
						subscriber.onNext(o);
						subscriber.onComplete();
					}
				};
			}
		})
		               .consumeNext();
	}

	@Override
	public void onComplete() {
		completeAccepted();
	}

	@Override
	public void onError(Throwable cause) {
		errorAccepted(cause);
	}

	@Override
	public void onNext(O element) {
		valueAccepted(element);
	}

	@Override
	public void onSubscribe(Subscription subscription) {
		if (BackpressureUtils.validate(this.subscription, subscription)) {
			this.subscription = subscription;
			if (requested == 1) {
				subscription.request(1L);
			}
		}
	}

	/**
	 * Block the calling thread, waiting for the completion of this {@code Promise}. A default timeout as specified in
	 * Reactor's {@link System#getProperties()} using the key {@code reactor.await.defaultTimeout} is used. The default
	 * is 30 seconds. If the promise is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has
	 * not completed
	 *
	 * @throws RuntimeException if the promise is completed with an error
	 */
	public O poll() {
		return poll(defaultTimeout, TimeUnit.MILLISECONDS);
	}

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this {@code Promise}. If the
	 * promise is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @param timeout the timeout value
	 * @param unit the {@link TimeUnit} of the timeout value
	 *
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has
	 * not completed
	 */
	public O poll(long timeout, TimeUnit unit) {
		try {
			return await(timeout, unit);
		}
		catch (InterruptedException ie) {
			Thread.currentThread()
			      .interrupt();
			return null;
		}
	}

	/**
	 * Return the error (if any) that has completed this {@code Promise}. Returns {@code null} if the promise has not
	 * been completed, or was completed with a value.
	 *
	 * @return the error (if any)
	 */
	public Throwable reason() {
		lock.lock();
		try {
			return error;
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public final void request(long n) {
		try {
			BackpressureUtils.checkRequest(n);
		}
		catch (Throwable e) {
			Exceptions.throwIfFatal(e);
			onError(e);
			return;
		}
		if (REQUESTED.compareAndSet(this, 0, 1)) {
			Subscription subscription = this.subscription;
			if (subscription != null) {
				subscription.request(1);
			}
		}
	}

	public Stream<O> stream() {
		Broadcaster<O> out = outboundStream;
		if (out == null) {
			lock.lock();
			out = outboundStream;
			try {
				if (finalState == FinalState.COMPLETE) {
					if (value == null) {
						return Streams.<O>empty();
					}
					else {
						return Streams.just(value);
					}

				}
				else if (finalState == FinalState.ERROR) {
					return Streams.fail(error);
				}
				else if (out == null) {
					out = Broadcaster.replayLastOrDefault(value, timer);
				}
				else {
					return out;
				}
				outboundStream = out;
			}
			finally {
				lock.unlock();
			}
		}

		if (out != null) {
			out.onSubscribe(this);
		}
		return outboundStream;
	}

	@Override
	public void subscribe(final Subscriber<? super O> subscriber) {
		stream().subscribe(subscriber);
	}

	@Override
	public String toString() {
		lock.lock();
		try {
			return "Promise{" +
					"value=" + value +
					(finalState != null ? ", state=" + finalState : "") +
					", error=" + error +
					'}';
		}
		finally {
			lock.unlock();
		}
	}

	@Override
	public final Object upstream() {
		return subscription;
	}

	protected void valueAccepted(O value) {
		Subscriber<O> subscriber;
		lock.lock();
		try {
			if (!isPending()) {
				Exceptions.onNextDropped(value);
			}
			this.value = value;
			this.finalState = FinalState.COMPLETE;

			subscriber = outboundStream;

			if (hasBlockers) {
				pendingCondition.signalAll();
				hasBlockers = false;
			}
		}
		finally {
			lock.unlock();
		}
		if (subscriber != null) {
			if (value != null) {
				subscriber.onNext(value);
			}
			subscriber.onComplete();
		}

		if (value != null) {
			Subscription s = subscription;
			if (s != null) {
				subscription = null;
				s.cancel();
			}
		}
	}

	public enum FinalState {
		ERROR,
		COMPLETE
	}
}