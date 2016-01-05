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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import javax.annotation.Nullable;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Mono;
import reactor.Timers;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.core.support.SignalType;
import reactor.core.support.internal.PlatformDependent;
import reactor.core.timer.Timer;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.rx.broadcast.Broadcaster;

/**
 * A {@code Promise} is a stateful event container that accepts a single value or error. In addition to {@link #get()
 * getting} or {@link #await() awaiting} the value, consumers can be registered to the outbound {@link #stream()} or via
 * , consumers can be registered to be notified of {@link #doOnError(Consumer) notified an error}, {@link
 * #doOnSuccess(Consumer) a value}, or {@link #doOnTerminate(BiConsumer)} both}. <p> A promise also provides methods for
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
	 *
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

	private final Timer        timer;
	protected     Subscription subscription;

	Broadcaster<O> outboundStream;

	volatile int requested = 0;
	volatile SignalType endState;
	volatile O          value;
	volatile Throwable  error;

	final static AtomicReferenceFieldUpdater<Promise, SignalType> STATE_UPDATER =
			PlatformDependent.newAtomicReferenceFieldUpdater(Promise.class, "endState");


	/**
	 * Creates a new unfulfilled promise
	 *
	 * @param timer The default Timer for time-sensitive downstream actions if any.
	 */
	Promise(@Nullable Timer timer) {
		this.timer = timer;
	}

	/**
	 * Creates a new promise that has been fulfilled with the given {@code value}.
	 *
	 * @param value The value that fulfills the promise
	 * @param timer The default Timer for time-sensitive downstream actions if any.
	 */
	Promise(O value, @Nullable Timer timer) {
		this.timer = timer;
		endState = value == null ? SignalType.COMPLETE : SignalType.NEXT;
		this.value = value;
	}

	/**
	 * Creates a new promise that has failed with the given {@code error}. <p> The {@code observable} is used when
	 * notifying the Promise's consumers, determining the thread on which they are called. If {@code env} is {@code
	 * null} the default await timeout will be 30 seconds.
	 *
	 * @param error The error the completed the promise
	 * @param timer The default Timer for time-sensitive downstream actions if any.
	 */
	Promise(Throwable error, @Nullable Timer timer) {
		this.timer = timer;
		endState = SignalType.ERROR;
		this.error = error;
	}

	/**
	 * Creates a new promise that has the given state. <p> The {@code observable} is used when notifying the Promise's
	 * consumers, determining the thread on which they are called. The given {@code env} is used to determine the
	 * default await timeout.
	 *
	 * @param state The state of the promise
	 * @param timer The default Timer for time-sensitive downstream actions if any.
	 */
	Promise(SignalType state, @Nullable Timer timer) {
		this(timer);
		this.endState = state;
	}

	@Override
	public void accept(O o) {
		onNext(o);
	}

	/**
	 * Block the calling thread, waiting for the completion of this {@code Promise}. A default timeout as specified in
	 * {@link System#getProperties()} using the key {@link #DEFAULT_TIMEOUT} is used. The default is 30 seconds. If the
	 * promise is completed with an error a RuntimeException that wraps the error is thrown.
	 *
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached and the {@code Promise} has
	 * not completed
	 *
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 * @throws RuntimeException     if the promise is completed with an error
	 */
	public O await() throws InterruptedException {
		return await(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
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

		long delay = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, unit);

		for (; ; ) {
			SignalType endState = this.endState;
			if (endState != null) {
				switch (endState) {
					case NEXT:
						return value;
					case ERROR:
						if (error instanceof RuntimeException) {
							throw (RuntimeException) error;
						}
						throw ReactorFatalException.create(error);
					case COMPLETE:
						return null;
				}
			}
			if (delay < System.currentTimeMillis()) {
				throw CancelException.get();
			}
			Thread.sleep(1);
		}
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
		return awaitSuccess(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
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

	@Override
	public Subscriber downstream() {
		return outboundStream;
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
		SignalType endState = this.endState;

		if (endState == SignalType.NEXT) {
			return value;
		}
		else if (endState == SignalType.ERROR) {
			if (RuntimeException.class.isInstance(error)) {
				throw (RuntimeException) error;
			}
			else {
				throw ReactorFatalException.create(error);
			}
		}
		else {
			return null;
		}
	}

	public Timer getTimer() {
		return timer;
	}


	/**
	 * Indicates whether this {@code Promise} has been completed with an error.
	 *
	 * @return {@code true} if this {@code Promise} was completed with an error, {@code false} otherwise.
	 */
	public boolean isError() {
		return endState == SignalType.ERROR;
	}

	/**
	 * Indicates whether this {@code Promise} has yet to be completed with a value or an error.
	 *
	 * @return {@code true} if this {@code Promise} is still pending, {@code false} otherwise.
	 *
	 * @see #isTerminated()
	 */
	public boolean isPending() {
		return endState == null;
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
		return endState == SignalType.NEXT || endState == SignalType.COMPLETE;
	}

	@Override
	public boolean isTerminated() {
		return endState == SignalType.COMPLETE || endState == SignalType.ERROR;
	}

	@Override
	public void onComplete() {
		onNext(null);
	}

	@Override
	public void onError(Throwable cause) {
		if (this.endState != null ||
				!STATE_UPDATER.compareAndSet(this, null, SignalType.ERROR)) {
				Exceptions.onErrorDropped(cause);
			return;
		}

		this.error = cause;
		subscription = null;

		if (outboundStream != null) {
			outboundStream.onError(error);
		}
	}

	@Override
	public void onNext(O value) {
		Subscriber<O> subscriber;

		SignalType endSignal = null == value ? SignalType.COMPLETE : SignalType.NEXT;

		if (this.endState != null ||
				!STATE_UPDATER.compareAndSet(this, null, endSignal)) {
			if (value != null) {
				Exceptions.onNextDropped(value);
			}
			return;
		}

		if(value != null) {
			this.value = value;
		}
		subscriber = outboundStream;

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
		return poll(DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
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
	 * Peek the error (if any) that has completed this {@code Promise}. Returns {@code null} if the promise has not been
	 * completed, or was completed with a value.
	 *
	 * @return the error (if any)
	 */
	public Throwable reason() {
		return error;
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
			SignalType endState = this.endState;
			if (endState == SignalType.COMPLETE) {
				return Streams.<O>empty();
			}
			else if(endState == SignalType.NEXT) {
				return Streams.just(value);
			}
			else if (endState == SignalType.ERROR) {
				return Streams.fail(error);
			}
			else {
				out = Broadcaster.replayLastOrDefault(value, timer);
			}
			outboundStream = out;
		}

		if (out != null){

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
		return "{" +
				"value : \"" + value + "\", " +
				(endState != null ? "state : \"" + endState + "\", " : "") +
				"error : \"" + error + "\" " +
				'}';
	}

	@Override
	public final Object upstream() {
		return subscription;
	}

}