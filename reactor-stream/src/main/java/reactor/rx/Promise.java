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
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Flux;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.ReactorFatalException;
import reactor.core.processor.ProcessorGroup;
import reactor.core.subscriber.SubscriberBarrier;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.ReactiveState;
import reactor.core.support.ReactiveStateUtils;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;
import reactor.core.timer.Timer;
import reactor.rx.broadcast.Broadcaster;

/**
 * A {@code Promise} is a stateful event container that accepts a single value or error.
 * In addition to {@link #get() getting} or {@link #await() awaiting} the value, consumers
 * can be registered to the outbound {@link #stream()} or via , consumers can be
 * registered to be notified of {@link #onError(Consumer) notified an error}, {@link
 * #onSuccess(Consumer) a value}, or {@link #onComplete(Consumer) both}. <p> A promise
 * also provides methods for composing actions with the future value much like a {@link
 * reactor.rx.Stream}. However, where a {@link reactor.rx.Stream} can process many values,
 * a {@code Promise} processes only one value or error.
 * @param <O> the type of the value that will be made available
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @see <a href="https://github.com/promises-aplus/promises-spec">Promises/A+
 * specification</a>
 */
public class Promise<O>
		implements Supplier<O>, Processor<O, O>, Consumer<O>, ReactiveState.Bounded, Subscription,
		           ReactiveState.Upstream, ReactiveState.Downstream, ReactiveState.ActiveUpstream {

	private volatile int requested = 0;

	private static final AtomicIntegerFieldUpdater<Promise> REQUESTED =
			AtomicIntegerFieldUpdater.newUpdater(Promise.class, "requested");

	private final ReentrantLock lock = new ReentrantLock();

	private final long defaultTimeout;

	private final Condition pendingCondition;

	private final Timer timer;

	Broadcaster<O> outboundStream;

	public enum FinalState {
		ERROR,
		COMPLETE
	}

	FinalState finalState = null;

	private O value;

	private Throwable error;

	private boolean hasBlockers = false;

	protected Subscription subscription;

	/**
	 * Creates a new unfulfilled promise. <p> The {@code dispatcher} is used when
	 * notifying the Promise's consumers, determining the thread on which they are called.
	 * The given {@code env} is used to determine the default await timeout. The default
	 * await timeout will be 30 seconds. This Promise will consumer errors from its {@code
	 * parent} such that if the parent completes in error then so too will this Promise.
	 */
	public Promise() {
		this(null);
	}

	/**
	 * Creates a new unfulfilled promise. <p> The {@code dispatcher} is used when
	 * notifying the Promise's consumers, determining the thread on which they are called.
	 * The given {@code env} is used to determine the default await timeout. If {@code
	 * env} is {@code null} the default await timeout will be 30 seconds. This Promise
	 * will consumer errors from its {@code parent} such that if the parent completes in
	 * error then so too will this Promise.
	 * @param timer The default Timer for time-sensitive downstream actions if any.
	 */
	public Promise(@Nullable Timer timer) {
		this.timer = timer;
		this.defaultTimeout = DEFAULT_TIMEOUT;
		this.pendingCondition = lock.newCondition();
	}

	/**
	 * Creates a new promise that has been fulfilled with the given {@code value}.
	 * @param value The value that fulfills the promise
	 * @param timer The default Timer for time-sensitive downstream actions if any.
	 */
	public Promise(O value, @Nullable Timer timer) {
		this(timer);
		finalState = FinalState.COMPLETE;
		this.value = value;
	}

	/**
	 * Creates a new promise that has failed with the given {@code error}. <p> The {@code
	 * observable} is used when notifying the Promise's consumers, determining the thread
	 * on which they are called. The given {@code env} is used to determine the default
	 * await timeout. If {@code env} is {@code null} the default await timeout will be 30
	 * seconds.
	 * @param error The error the completed the promise
	 * @param timer The default Timer for time-sensitive downstream actions if any.
	 */
	public Promise(Throwable error, Timer timer) {
		this(timer);
		finalState = FinalState.ERROR;
		this.error = error;
	}

	/**
	 * Assign a {@link Consumer} that will either be invoked later, when the {@code
	 * Promise} is completed by either setting a value or propagating an error, or, if
	 * this {@code Promise} has already been fulfilled, is immediately scheduled to be
	 * executed.
	 * @param onComplete the completion {@link Consumer}
	 * @return {@literal the new Promise}
	 */
	public final Promise<O> onComplete(@Nonnull final Consumer<Promise<O>> onComplete) {
		request(1);
		lock.lock();
		try {
			if (finalState == FinalState.ERROR) {
				onComplete.accept(this);
				return Promises.error(timer, error);
			}
			else if (finalState == FinalState.COMPLETE) {
				onComplete.accept(this);
				return Promises.success(timer, value);
			}
		}
		catch (Throwable t) {
			return Promises.error(timer, t);
		}
		finally {
			lock.unlock();
		}

		return stream().lift(new Flux.Operator<O, O>() {
			@Override
			public Subscriber<? super O> apply(final Subscriber<? super O> subscriber) {
				return new SubscriberBarrier<O, O>(subscriber){
					@Override
					protected void doNext(O o) {
						doCancel();
						onComplete.accept(Promise.this);
						subscriber.onNext(o);
						subscriber.onComplete();
					}

					@Override
					protected void doError(Throwable throwable) {
						onComplete.accept(Promise.this);
						subscriber.onError(throwable);
					}

					@Override
					protected void doComplete() {
						onComplete.accept(Promise.this);
						subscriber.onComplete();
					}
				};
			}
		}).consumeNext();
	}

	/**
	 * Assign a {@link Consumer} that will either be invoked later, when the {@code
	 * Promise} is successfully completed with a value, or, if this {@code Promise} has
	 * already been fulfilled, is immediately executed.
	 * @param onSuccess the success {@link Consumer}
	 * @return {@literal the new Promise}
	 */
	public final Promise<O> onSuccess(@Nonnull final Consumer<O> onSuccess) {
		request(1);
		lock.lock();
		try {
			if (finalState == FinalState.COMPLETE) {
				if (value != null) {
					onSuccess.accept(value);
				}
				return this;
			}
		}
		catch (Throwable t) {
			return Promises.error(timer, t);
		}
		finally {
			lock.unlock();
		}
		return stream().observe(onSuccess).consumeNext();
	}

	/**
	 * Assign a {@link Function} that will either be invoked later, when the {@code
	 * Promise} is successfully completed with a value, or, if this {@code Promise} has
	 * already been fulfilled, is immediately scheduled to be executed.
	 * @param transformation the function to apply on signal to the transformed Promise
	 * @return {@literal the new Promise}
	 */
	public final <V> Promise<V> map(@Nonnull final Function<? super O, ? extends V> transformation) {
		lock.lock();
		try {
			if (finalState == FinalState.ERROR) {
				return Promises.error(timer, error);
			}
			else if (finalState == FinalState.COMPLETE) {
				return Promises.success(timer,
						value != null ? transformation.apply(value) : null);
			}
		}
		catch (Throwable t) {
			return Promises.error(timer, t);
		}
		finally {
			lock.unlock();
		}
		return (Promise<V>) stream().map(transformation).next();
	}

	/**
	 * Assign a {@link Function} that will either be invoked later, when the {@code
	 * Promise} is successfully completed with a value, or, if this {@code Promise} has
	 * already been fulfilled, is immediately scheduled to be executed. <p> FlatMap is
	 * typically used to listen for a delayed/async publisher, e.g. promise.flatMap( data
	 * -> Promise.success (data) ). The result is merged directly on the returned stream.
	 * @param transformation the function to apply on signal to the supplied Promise that
	 * will be merged back.
	 * @return {@literal the new Promise}
	 */
	public final <V> Promise<V> flatMap(
			@Nonnull final Function<? super O, ? extends Publisher<? extends V>> transformation) {
		lock.lock();
		try {
			if (finalState == FinalState.ERROR) {
				return Promises.error(timer, error);
			}
			else if (finalState == FinalState.COMPLETE) {
				if (value != null) {
					Promise<V> successPromise = Promises.<V>ready(timer);
					transformation.apply(value).subscribe(successPromise);
					return successPromise;
				}
				else {
					return Promises.success(timer, null);
				}
			}
		}
		catch (Throwable t) {
			return Promises.error(timer, t);
		}
		finally {
			lock.unlock();
		}
		return (Promise<V>) stream().flatMap(transformation).next();

	}

	/**
	 * Assign a {@link Consumer} that will either be invoked later, when the {@code
	 * Promise} is completed with an error, or, if this {@code Promise} has already been
	 * fulfilled, is immediately scheduled to be executed. The error is recovered and
	 * materialized as the next signal to the returned stream.
	 * @param onError the error {@link Consumer}
	 * @return {@literal the new Promise}
	 */
	public final Promise<O> onError(@Nonnull final Consumer<Throwable> onError) {
		request(1);
		lock.lock();
		try {
			if (finalState == FinalState.ERROR) {
				onError.accept(error);
				return this;
			}
			else if (finalState == FinalState.COMPLETE) {
				return this;
			}
		}
		catch (Throwable t) {
			return Promises.error(timer, t);
		}
		finally {
			lock.unlock();
		}

		return stream().when(Throwable.class, onError).consumeNext();
	}

	/**

	 */
	@SuppressWarnings("unchecked")
	public final <E> Promise<E> process(final Processor<O, E> processor) {
		subscribe(processor);
		if (Promise.class.isAssignableFrom(processor.getClass())) {
			return (Promise<E>) processor;
		}
		Promise<E> promise = Promises.ready(getTimer());
		processor.subscribe(promise);
		return promise;
	}

	/**
	 */
	public final Promise<O> publishOn(final ProcessorGroup<O> processorProvider) {
		return stream().publishOn(processorProvider).next();
	}

	/**
	 */
	public final Promise<O> dispatchOn(final ProcessorGroup<O> processorProvider) {
		return stream().dispatchOn(processorProvider).next();
	}

	/**
	 * Only forward onError and onComplete signals into the returned stream.
	 * @return {@literal new Promise}
	 */
	public final Promise<Void> after() {
		lock.lock();
		try {
			if (finalState == FinalState.COMPLETE) {
				return Promises.<Void>success(timer, null);
			}
		}
		catch (Throwable t) {
			return Promises.<Void>error(timer, t);
		}
		finally {
			lock.unlock();
		}
		return stream().after().next();
	}

	@Override
	public final Object upstream() {
		return subscription;
	}

	/**
	 * Indicates whether this {@code Promise} has been completed with either an error or a
	 * value
	 * @return {@code true} if this {@code Promise} is complete, {@code false} otherwise.
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
	 * Indicates whether this {@code Promise} has yet to be completed with a value or an
	 * error.
	 * @return {@code true} if this {@code Promise} is still pending, {@code false}
	 * otherwise.
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

	/**
	 * Indicates whether this {@code Promise} has been successfully completed a value.
	 * @return {@code true} if this {@code Promise} is successful, {@code false}
	 * otherwise.
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

	/**
	 * Indicates whether this {@code Promise} has been completed with an error.
	 * @return {@code true} if this {@code Promise} was completed with an error, {@code
	 * false} otherwise.
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
	 * Block the calling thread, waiting for the completion of this {@code Promise}. A
	 * default timeout as specified in {@link System#getProperties()} using the key {@code
	 * reactor.await.defaultTimeout} is used. The default is 30 seconds. If the promise is
	 * completed with an error a RuntimeException that wraps the error is thrown.
	 * @return true if complete without error
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 * @throws RuntimeException     if the promise is completed with an error
	 */
	public boolean awaitSuccess() throws InterruptedException {
		return awaitSuccess(defaultTimeout, TimeUnit.MILLISECONDS);
	}

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this
	 * {@code Promise}.
	 * @param timeout the timeout value
	 * @param unit the {@link TimeUnit} of the timeout value
	 * @return true if complete without error completed
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 */
	public boolean awaitSuccess(long timeout, TimeUnit unit) throws InterruptedException {
		await(timeout, unit);
		return isSuccess();
	}

	/**
	 * Block the calling thread, waiting for the completion of this {@code Promise}. A
	 * default timeout as specified in {@link System#getProperties()} using the key {@code
	 * reactor.await.defaultTimeout} is used. The default is 30 seconds. If the promise is
	 * completed with an error a RuntimeException that wraps the error is thrown.
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached
	 * and the {@code Promise} has not completed
	 * @throws InterruptedException if the thread is interruped while awaiting completion
	 * @throws RuntimeException     if the promise is completed with an error
	 */
	public O await() throws InterruptedException {
		return await(defaultTimeout, TimeUnit.MILLISECONDS);
	}

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this
	 * {@code Promise}.
	 * @param timeout the timeout value
	 * @param unit the {@link TimeUnit} of the timeout value
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached
	 * and the {@code Promise} has not completed
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
	 * Block the calling thread, waiting for the completion of this {@code Promise}. A
	 * default timeout as specified in Reactor's {@link System#getProperties()} using the
	 * key {@code reactor.await.defaultTimeout} is used. The default is 30 seconds. If the
	 * promise is completed with an error a RuntimeException that wraps the error is
	 * thrown.
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached
	 * and the {@code Promise} has not completed
	 * @throws RuntimeException if the promise is completed with an error
	 */
	public O poll() {
		return poll(defaultTimeout, TimeUnit.MILLISECONDS);
	}

	/**
	 * Block the calling thread for the specified time, waiting for the completion of this
	 * {@code Promise}. If the promise is completed with an error a RuntimeException that
	 * wraps the error is thrown.
	 * @param timeout the timeout value
	 * @param unit the {@link TimeUnit} of the timeout value
	 * @return the value of this {@code Promise} or {@code null} if the timeout is reached
	 * and the {@code Promise} has not completed
	 */
	public O poll(long timeout, TimeUnit unit) {
		try {
			return await(timeout, unit);
		}
		catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			return null;
		}
	}

	/**
	 * Returns the value that completed this promise. Returns {@code null} if the promise
	 * has not been completed. If the promise is completed with an error a
	 * RuntimeException that wraps the error is thrown.
	 * @return the value that completed the promise, or {@code null} if it has not been
	 * completed
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

	/**
	 * Return the error (if any) that has completed this {@code Promise}. Returns {@code
	 * null} if the promise has not been completed, or was completed with a value.
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
				else if (out == null){
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

	public Timer getTimer() {
		return timer;
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

	@Override
	public void onNext(O element) {
		valueAccepted(element);
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
	public void accept(O o) {
		valueAccepted(o);
	}

	public ReactiveStateUtils.Graph debug() {
		return ReactiveStateUtils.scan(this);
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

	protected void valueAccepted(O value) {
		Subscriber<O> subscriber;
		lock.lock();
		try {
			if (!isPending()) {
				throw CancelException.get();
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
		if (subscriber != null ) {
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

	/**
	 *
	 */
	public final void fulfill(){
		request(1L);
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

	@Override
	public void cancel() {
		Subscription subscription = this.subscription;
		if (subscription != null) {
			this.subscription = null;
			subscription.cancel();
		}
	}

	@Override
	public final long getCapacity() {
		return 1;
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
	public Subscriber downstream() {
		return outboundStream;
	}

	@Override
	public boolean isStarted() {
		return requested > 0;
	}

	@Override
	public boolean isTerminated() {
		return finalState == FinalState.COMPLETE;
	}
}