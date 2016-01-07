/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.support;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.error.Exceptions;
import reactor.core.error.InsufficientCapacityException;
import reactor.core.subscription.CancelledSubscription;
import reactor.core.support.rb.disruptor.Sequence;
import reactor.fn.BooleanSupplier;

/**
 * A generic utility to check subscription, request size and to cap concurrent additive operations to Long
 * .MAX_VALUE_LONG,
 * which is generic to {@link org.reactivestreams.Subscription#request(long)} handling.
 *
 * Combine utils available to operator implementations, @see http://github.com/reactor/reactive-streams-commons
 *
 * @author Stephane Maldini
 *
 * @since 2.5
 */
public enum BackpressureUtils {
	;


	static final long COMPLETED_MASK = 0x8000_0000_0000_0000L;
	static final long REQUESTED_MASK = 0x7FFF_FFFF_FFFF_FFFFL;

	/**
	 * Check Subscription current state and cancel new Subscription if different null, returning true if
	 * ready to subscribe.
	 *
	 * @param current current Subscription, expected to be null
	 * @param next new Subscription
	 * @return true if Subscription can be used
	 */
	public static boolean validate(Subscription current, Subscription next) {
		Objects.requireNonNull(next, "Subscription cannot be null");
		if (current != null) {
			next.cancel();
			//reportSubscriptionSet();
			return false;
		}

		return true;
	}

	/**
	 * Throws an exception if request is 0 or negative as specified in rule 3.09 of Reactive Streams
	 *
	 * @param n demand to check
	 * @throws IllegalArgumentException
	 */
	public static void checkRequest(long n) throws IllegalArgumentException {
		if (n <= 0L) {
			throw Exceptions.spec_3_09_exception(n);
		}
	}


	/**
	 * Throws an exception if request is 0 or negative as specified in rule 3.09 of Reactive Streams
	 *
	 * @param n          demand to check
	 * @param subscriber Subscriber to onError if non strict positive n
	 *
	 * @return true if valid or false if specification exception occured
	 *
	 * @throws IllegalArgumentException if subscriber is null and demand is negative or 0.
	 */
	public static boolean checkRequest(long n, Subscriber<?> subscriber) {
		if (n <= 0L) {
			if (null != subscriber) {
				subscriber.onError(Exceptions.spec_3_09_exception(n));
			} else {
				throw Exceptions.spec_3_09_exception(n);
			}
			return false;
		}
		return true;
	}

	/**
	 * Cap a multiplication to Long.MAX_VALUE
	 *
	 * @param a left operand
	 * @param b right operand
	 * @return Product result or Long.MAX_VALUE if overflow
	 */
	public static long multiplyCap(long a, long b) {
		long u = a * b;
		if (((a | b) >>> 31) != 0) {
			if (u / a != b) {
				return Long.MAX_VALUE;
			}
		}
		return u;
	}

	/**
	 * Cap an addition to Long.MAX_VALUE
	 *
	 * @param a left operand
	 * @param b right operand
	 * @return Addition result or Long.MAX_VALUE if overflow
	 */
	public static long addCap(long a, long b) {
		long res = a + b;
		if (res < 0L) {
			return Long.MAX_VALUE;
		}
		return res;
	}

	/**
	 * Cap a substraction to 0
	 *
	 * @param a left operand
	 * @param b right operand
	 * @return Subscription result or 0 if overflow
	 */
	public static long subOrZero(long a, long b) {
		long res = a - b;
		if (res < 0L) {
			return 0;
		}
		return res;
	}


	/**
	 * Cap a substraction to 0
	 *
	 * @param a left operand
	 * @param b right operand
	 * @return Subscription result or 0 if overflow
	 */
	public static int subOrZero(int a, int b) {
		int res = a - b;
		if (res < 0) {
			return 0;
		}
		return res;
	}


	/**
	 * Concurrent addition bound to Long.MAX_VALUE.
	 * Any concurrent write will "happen" before this operation.
	 *
	 * @param current current atomic to update
	 * @param toAdd   delta to add
	 * @return Addition result or Long.MAX_VALUE
	 */
	public static long addAndGet(AtomicLong current, long toAdd) {
		long u, r;
		do {
			r = current.get();
			if (r == Long.MAX_VALUE) {
				return Long.MAX_VALUE;
			}
			u = addCap(r, toAdd);
		} while (!current.compareAndSet(r, u));

		return u;
	}

	/**
	 *
	 * @param updater
	 * @param instance
	 * @param n
	 * @param <T>
	 * @return
	 */
	public static <T> long addAndGet(AtomicLongFieldUpdater<T> updater, T instance, long n) {
		for (;;) {
			long r = updater.get(instance);
			if (r == Long.MAX_VALUE) {
				return Long.MAX_VALUE;
			}
			long u = addCap(r, n);
			if (updater.compareAndSet(instance, r, u)) {
				return r;
			}
		}
	}

	/**
	 * Concurrent addition bound to Long.MAX_VALUE.
	 * Any concurrent write will "happen" before this operation.
	 *
	 * @param updater  current field updater
	 * @param instance current instance to update
	 * @param toAdd    delta to add
	 * @return Addition result or Long.MAX_VALUE
	 */
	public static <T> long getAndAdd(AtomicLongFieldUpdater<T> updater, T instance, long toAdd) {
		long r, u;
		do {
			r = updater.get(instance);
			if (r == Long.MAX_VALUE) {
				return Long.MAX_VALUE;
			}
			u = addCap(r, toAdd);
		} while (!updater.compareAndSet(instance, r, u));

		return r;
	}

	/**
	 * Concurrent addition bound to Long.MAX_VALUE.
	 * Any concurrent write will "happen" before this operation.
	 *
	 * @param sequence current sequence to update
	 * @param toAdd    delta to add
	 * @return Addition result or Long.MAX_VALUE
	 */
	public static long getAndAdd(Sequence sequence, long toAdd) {
		long u, r;
		do {
			r = sequence.get();
			if (r == Long.MAX_VALUE) {
				return Long.MAX_VALUE;
			}
			u = addCap(r, toAdd);
		} while (!sequence.compareAndSet(r, u));
		return r;
	}
	/**
	 * Concurrent substraction bound to 0.
	 * Any concurrent write will "happen" before this operation.
	 *
	 * @param updater  current field updater
	 * @param instance current instance to update
	 * @param toSub    delta to sub
	 * @return Substraction result or zero
	 */
	public static <T> long getAndSub(AtomicLongFieldUpdater<T> updater, T instance, long toSub) {
		long r, u;
		do {
			r = updater.get(instance);
			if (r == 0 || r == Long.MAX_VALUE) {
				return r;
			}
			u = subOrZero(r, toSub);
		} while (!updater.compareAndSet(instance, r, u));

		return r;
	}

	/**
	 * Concurrent substraction bound to 0.
	 * Any concurrent write will "happen" before this operation.
	 *
	 * @param updater  current field updater
	 * @param instance current instance to update
	 * @param toSub    delta to sub
	 * @return Substraction result or zero
	 */
	public static <T> long getAndSub(AtomicIntegerFieldUpdater<T> updater, T instance, int toSub) {
		int r, u;
		do {
			r = updater.get(instance);
			if (r == 0) {
				return r;
			}
			u = subOrZero(r, toSub);
		} while (!updater.compareAndSet(instance, r, u));

		return r;
	}

	/**
	 * Concurrent substraction bound to 0 and Long.MAX_VALUE.
	 * Any concurrent write will "happen" before this operation.
	 *
	 * @param sequence current sequence to update
	 * @param toSub    delta to sub
	 * @return Substraction result, 0 or Long.MAX_VALUE
	 */
	public static long getAndSub(Sequence sequence, long toSub) {
		long r, u;
		do {
			r = sequence.get();
			if (r == 0 || r == Long.MAX_VALUE) {
				return r;
			}
			u = subOrZero(r, toSub);
		} while (!sequence.compareAndSet(r, u));

		return r;
	}

	/**
	 * Concurrent substraction bound to 0 and Long.MAX_VALUE.
	 * Any concurrent write will "happen" before this operation.
	 *
	 * @param sequence current atomic to update
	 * @param toSub    delta to sub
	 * @return Substraction result, 0 or Long.MAX_VALUE
	 */
	public static long getAndSub(AtomicLong sequence, long toSub) {
		long r, u;
		do {
			r = sequence.get();
			if (r == 0 || r == Long.MAX_VALUE) {
				return r;
			}
			u = subOrZero(r, toSub);
		} while (!sequence.compareAndSet(r, u));

		return r;
	}

	public static <F> boolean terminate(AtomicReferenceFieldUpdater<F, Subscription> field, F instance) {
		Subscription a = field.get(instance);
		if (a != CancelledSubscription.INSTANCE) {
			a = field.getAndSet(instance, CancelledSubscription.INSTANCE);
			if (a != null && a != CancelledSubscription.INSTANCE) {
				a.cancel();
				return true;
			}
		}
		return false;
	}

	public static <F> boolean setOnce(AtomicReferenceFieldUpdater<F, Subscription> field, F instance, Subscription s) {
		Subscription a = field.get(instance);
		if (a == CancelledSubscription.INSTANCE) {
			return false;
		}
		if (a != null) {
			reportSubscriptionSet();
			return false;
		}

		if (field.compareAndSet(instance, null, s)) {
			return true;
		}

		a = field.get(instance);

		if (a == CancelledSubscription.INSTANCE) {
			return false;
		}

		reportSubscriptionSet();
		return false;
	}

	/**
	 *
	 */
	public static void reportSubscriptionSet() {
		throw Exceptions.spec_2_13_exception();
	}

	/**
	 *
	 * @param n
	 */
	public static void reportBadRequest(long n) {
		throw Exceptions.spec_3_09_exception(n);
	}

	/**
	 *
	 */
	public static void reportMoreProduced() {
		throw InsufficientCapacityException.get();
	}

	/**
	 *
	 * @param n
	 * @return
	 */
	public static boolean validate(long n) {
		if (n < 0) {
			reportBadRequest(n);
			return false;
		}
		return true;
	}/**
	 * Perform a potential post-completion request accounting.
	 *
	 * @param n
	 * @param actual
	 * @param queue
	 * @param field
	 * @param isCancelled
	 * @return true if the state indicates a completion state.
	 */
	public static <T, F> boolean postCompleteRequest(long n,
			Subscriber<? super T> actual,
			Queue<T> queue,
			AtomicLongFieldUpdater<F> field,
			F instance,
			BooleanSupplier isCancelled) {

		for (; ; ) {
			long r = field.get(instance);

			// extract the current request amount
			long r0 = r & REQUESTED_MASK;

			// preserve COMPLETED_MASK and calculate new requested amount
			long u = (r & COMPLETED_MASK) | addCap(r0, n);

			if (field.compareAndSet(instance, r, u)) {
				// (complete, 0) -> (complete, n) transition then replay
				if (r == COMPLETED_MASK) {

					postCompleteDrain(n | COMPLETED_MASK, actual, queue, field, instance, isCancelled);

					return true;
				}
				// (active, r) -> (active, r + n) transition then continue with requesting from upstream
				return false;
			}
		}

	}

	/**
	 * Drains the queue either in a pre- or post-complete state.
	 *
	 * @param n
	 * @param actual
	 * @param queue
	 * @param field
	 * @param isCancelled
	 * @return true if the queue was completely drained or the drain process was cancelled
	 */
	static <T, F> boolean postCompleteDrain(long n,
			Subscriber<? super T> actual,
			Queue<T> queue,
			AtomicLongFieldUpdater<F> field,
			F instance,
			BooleanSupplier isCancelled) {

// TODO enable fast-path
//        if (n == -1 || n == Long.MAX_VALUE) {
//            for (;;) {
//                if (isCancelled.getAsBoolean()) {
//                    break;
//                }
//
//                T v = queue.poll();
//
//                if (v == null) {
//                    actual.onComplete();
//                    break;
//                }
//
//                actual.onNext(v);
//            }
//
//            return true;
//        }

		long e = n & COMPLETED_MASK;

		for (; ; ) {

			while (e != n) {
				if (isCancelled.getAsBoolean()) {
					return true;
				}

				T t = queue.poll();

				if (t == null) {
					actual.onComplete();
					return true;
				}

				actual.onNext(t);
				e++;
			}

			if (isCancelled.getAsBoolean()) {
				return true;
			}

			if (queue.isEmpty()) {
				actual.onComplete();
				return true;
			}

			n = field.get(instance);

			if (n == e) {

				n = field.addAndGet(instance, -(e & REQUESTED_MASK));

				if ((n & REQUESTED_MASK) == 0L) {
					return false;
				}

				e = n & COMPLETED_MASK;
			}
		}

	}

	/**
	 * Tries draining the queue if the source just completed.
	 *
	 * @param actual
	 * @param queue
	 * @param field
	 * @param isCancelled
	 */
	public static <T, F> void postComplete(Subscriber<? super T> actual,
			Queue<T> queue,
			AtomicLongFieldUpdater<F> field,
			F instance,
			BooleanSupplier isCancelled) {

		if (queue.isEmpty()) {
			actual.onComplete();
			return;
		}

		if (postCompleteDrain(field.get(instance), actual, queue, field, instance, isCancelled)) {
			return;
		}

		for (; ; ) {
			long r = field.get(instance);

			if ((r & COMPLETED_MASK) != 0L) {
				return;
			}

			long u = r | COMPLETED_MASK;
			// (active, r) -> (complete, r) transition
			if (field.compareAndSet(instance, r, u)) {
				// if the requested amount was non-zero, drain the queue
				if (r != 0L) {
					postCompleteDrain(u, actual, queue, field, instance, isCancelled);
				}

				return;
			}
		}
	}
}
