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
package reactor.core.processor.rb.disruptor.util;

import reactor.core.processor.rb.disruptor.Sequence;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;

/**
 * Set of common functions used by the Disruptor
 */
public final class Util {

	public static boolean isPowerOfTwo(final int x){
		return Integer.bitCount(x) == 1;
	}
	/**
	 * Calculate the next power of 2, greater than or equal to x.<p>
	 * From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
	 *
	 * @param x Value to round up
	 * @return The next power of 2 from x inclusive
	 */
	public static int ceilingNextPowerOfTwo(final int x) {
		return 1 << (32 - Integer.numberOfLeadingZeros(x - 1));
	}

	/**
	 * Get the minimum sequence from an array of {@link reactor.core.processor.rb.disruptor.Sequence}s.
	 *
	 * @param sequences to compare.
	 * @return the minimum sequence found or Long.MAX_VALUE if the array is empty.
	 */
	public static long getMinimumSequence(final Sequence[] sequences) {
		return getMinimumSequence(sequences, Long.MAX_VALUE);
	}

	/**
	 * Get the minimum sequence from an array of {@link reactor.core.processor.rb.disruptor.Sequence}s.
	 *
	 * @param sequences to compare.
	 * @param minimum   an initial default minimum.  If the array is empty this value will be
	 *                  returned.
	 * @return the minimum sequence found or Long.MAX_VALUE if the array is empty.
	 */
	public static long getMinimumSequence(final Sequence[] sequences, long minimum) {
		for (int i = 0, n = sequences.length; i < n; i++) {
			long value = sequences[i].get();
			minimum = Math.min(minimum, value);
		}

		return minimum;
	}

	/**
	 * Get the minimum sequence from an array of {@link reactor.core.processor.rb.disruptor.Sequence}s.
	 *
	 * @param excludeSequence to exclude from search.
	 * @param sequences to compare.
	 * @param minimum   an initial default minimum.  If the array is empty this value will be
	 *                  returned.
	 * @return the minimum sequence found or Long.MAX_VALUE if the array is empty.
	 */
	public static long getMinimumSequence(Sequence excludeSequence, final Sequence[] sequences, long minimum) {
		for (int i = 0, n = sequences.length; i < n; i++) {
			if (excludeSequence == null || sequences[i] != excludeSequence) {
				long value = sequences[i].get();
				minimum = Math.min(minimum, value);
			}
		}

		return minimum;
	}

	private static final Unsafe THE_UNSAFE;

	static {
		try {
			final PrivilegedExceptionAction<Unsafe> action = new PrivilegedExceptionAction<Unsafe>() {
				public Unsafe run() throws Exception {
					Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
					theUnsafe.setAccessible(true);
					return (Unsafe) theUnsafe.get(null);
				}
			};

			THE_UNSAFE = AccessController.doPrivileged(action);
		} catch (Exception e) {
			throw new RuntimeException("Unable to load unsafe", e);
		}
	}

	/**
	 * Get a handle on the Unsafe instance, used for accessing low-level concurrency
	 * and memory constructs.
	 *
	 * @return The Unsafe
	 */
	public static Unsafe getUnsafe() {
		return THE_UNSAFE;
	}

	/**
	 * Gets the address value for the memory that backs a direct byte buffer.
	 *
	 * @param buffer
	 * @return The system address for the buffers
	 */
	public static long getAddressFromDirectByteBuffer(ByteBuffer buffer) {
		try {
			Field addressField = Buffer.class.getDeclaredField("address");
			addressField.setAccessible(true);
			return addressField.getLong(buffer);
		} catch (Exception e) {
			throw new RuntimeException("Unable to address field from ByteBuffer", e);
		}
	}


	/**
	 * Calculate the log base 2 of the supplied integer, essentially reports the location
	 * of the highest bit.
	 *
	 * @param i Value to calculate log2 for.
	 * @return The log2 value
	 */
	public static int log2(int i) {
		int r = 0;
		while ((i >>= 1) != 0) {
			++r;
		}
		return r;
	}
}
