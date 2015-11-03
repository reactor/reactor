/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.subscription;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.Publishers;
import reactor.core.error.CancelException;
import reactor.core.error.Exceptions;
import reactor.core.error.InsufficientCapacityException;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.Bounded;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public class ReactiveSession<E> implements Subscription, Bounded {

	public enum Emission {
		FAILED, BACKPRESSURED, OK, DROPPED, CANCELLED
	}

	private final Subscriber<? super E> actual;

	@SuppressWarnings("unused")
	private volatile long                                    requested = 0L;
	@SuppressWarnings("rawtypes")
	static final     AtomicLongFieldUpdater<ReactiveSession> REQUESTED =
			AtomicLongFieldUpdater.newUpdater(ReactiveSession.class, "requested");

	private Throwable uncaughtException;

	private volatile boolean cancelled;

	/**
	 *
	 * @param subscriber
	 * @param <E>
	 * @return
	 */
	public static <E> ReactiveSession<E> create(Subscriber<? super E> subscriber) {
		return create(subscriber, true);
	}

	/**
	 *
	 * @param subscriber
	 * @param autostart
	 * @param <E>
	 * @return
	 */
	public static <E> ReactiveSession<E> create(Subscriber<? super E> subscriber, boolean autostart) {
		ReactiveSession<E> sub = new ReactiveSession<>(subscriber);
		if (autostart) {
			sub.start();
		}
		return sub;
	}

	protected ReactiveSession(Subscriber<? super E> actual) {
		this.actual = actual;
	}

	/**
	 *
	 */
	public void start() {
		try {
			actual.onSubscribe(this);
		}
		catch (Throwable t) {
			Publishers.<E>error(t).subscribe(actual);
			uncaughtException = t;
		}
	}

	/**
	 *
	 * @param data
	 * @return
	 */
	public Emission emit(E data) {
		if (cancelled) {
			return Emission.CANCELLED;
		}
		if (uncaughtException != null) {
			return Emission.FAILED;
		}
		try {
			if (BackpressureUtils.getAndSub(REQUESTED, this, 1L) == 0L) {
				return Emission.BACKPRESSURED;
			}
			actual.onNext(data);
			return Emission.OK;
		}
		catch (CancelException ce) {
			return Emission.CANCELLED;
		}
		catch (InsufficientCapacityException ice) {
			return Emission.BACKPRESSURED;
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			uncaughtException = t;
			return Emission.FAILED;
		}
	}

	/**
	 *
	 * @param error
	 */
	public void failWith(Throwable error) {
		if (uncaughtException == null) {
			uncaughtException = error;
			actual.onError(error);
		}
		else {
			IllegalStateException ise = new IllegalStateException("Session already failed");
			Exceptions.addCause(ise, error);
			throw ise;
		}
	}

	/**
	 *
	 * @return
	 */
	public Emission end(){
		if (cancelled) {
			return Emission.CANCELLED;
		}
		if (uncaughtException != null) {
			return Emission.FAILED;
		}
		try{
			cancelled = true;
			actual.onComplete();
			return Emission.OK;
		}
		catch (CancelException ce) {
			return Emission.CANCELLED;
		}
		catch (InsufficientCapacityException ice) {
			return Emission.BACKPRESSURED;
		}
		catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			uncaughtException = t;
			return Emission.FAILED;
		}
	}

	/**
	 *
	 * @return
	 */
	public boolean hasFailed() {
		return uncaughtException != null;
	}


	/**
	 *
	 * @return
	 */
	public boolean hasEnded() {
		return cancelled;
	}

	/**
	 *
	 * @return
	 */
	public Throwable getError() {
		return uncaughtException;
	}

	@Override
	public void request(long n) {
		if (BackpressureUtils.checkRequest(n, actual)) {
			BackpressureUtils.getAndAdd(REQUESTED, this, n);
		}
	}

	@Override
	public void cancel() {
		cancelled = true;
	}

	@Override
	public boolean isExposedToOverflow(Bounded parentPublisher) {
		return Bounded.class.isAssignableFrom(actual.getClass()) && ((Bounded) actual).isExposedToOverflow(parentPublisher);
	}

	@Override
	public long getCapacity() {
		return Bounded.class.isAssignableFrom(actual.getClass()) ? ((Bounded) actual).getCapacity() : Long.MAX_VALUE;
	}
}
