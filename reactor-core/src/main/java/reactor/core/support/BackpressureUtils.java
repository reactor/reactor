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

package reactor.core.support;

import reactor.core.error.SpecificationExceptions;
import reactor.core.processor.rb.disruptor.Sequence;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * A generic utility to check request size and to cap concurrent additive operations to Long.MAX_VALUE_LONG,
 * which is generic to {@link org.reactivestreams.Subscription#request(long)} handling.
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public abstract class BackpressureUtils {

	/**
	 * Throws an exception if request is 0 or negative as specified in rule 3.09 of Reactive Streams
	 *
	 * @param n demand to check
	 * @throws IllegalArgumentException
	 */
	public static void checkRequest(long n) throws IllegalArgumentException {
		if (n <= 0L) {
			throw SpecificationExceptions.spec_3_09_exception(n);
		}
	}

	/**
	 * Cap a multiplication to Long.MAX_VALUE
	 *
	 * @param a left operand
	 * @param b right operand
	 * @return Product result or Long.MAX_VALUE if overflow
	 */
	public static long multiplyOrLongMax(long a, long b) {
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
	public static long addOrLongMax(long a, long b) {
		long res = a + b;
		if (res < 0L) {
			return Long.MAX_VALUE;
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
	public static long getAndAdd(AtomicLong current, long toAdd) {
		long u, r;
		do {
			r = current.get();
			if (r == Long.MAX_VALUE) {
				return Long.MAX_VALUE;
			}
			u = addOrLongMax(r, toAdd);
		} while (!current.compareAndSet(r, u));

		return r;
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
			u = addOrLongMax(r, toAdd);
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
			u = addOrLongMax(r, toAdd);
		} while (!sequence.compareAndSet(r, u));
		return r;
	}
}
