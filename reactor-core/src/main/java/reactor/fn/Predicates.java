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

package reactor.fn;

import reactor.core.support.Assert;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class Predicates {

	protected Predicates() {
	}

	/**
	 * Returns a {@literal Predicate} which always evaluates to false.
	 *
	 * @param <T> the type handled by the predicate
	 * @return the never predicate
	 */
	public static <T> Predicate<T> never(){
		return new Predicate<T>() {
			@Override
			public boolean test(T o) {
				return false;
			}
		};
	}

	/**
	 * Returns a {@literal Predicate} which always evaluates to true.
	 *
	 * @param <T> the type handled by the predicate
	 * @return the always predicate
	 */
	public static <T> Predicate<T> always(){
		return new Predicate<T>() {
			@Override
			public boolean test(T o) {
				return true;
			}
		};
	}

	/**
	 * Returns a {@literal Predicate} which evaluates to {@literal true} only if all the provided predicates evaluate to
	 * {@literal true}.
	 *
	 * @param predicates
	 * 		{@literal Predicate Predicates} which will be ANDed together.
	 *
	 * @return A new {@literal Predicate} which returns {@literal true} only if all {@literal Predicate Predicates}
	 * return
	 * {@literal true}.
	 */
	public static <T> Predicate<T> and(final Predicate<? super T>... predicates) {
		Assert.notEmpty(predicates, "Predicate array cannot be empty.");
		return new Predicate<T>() {
			@Override
			public boolean test(T t) {
				for (Predicate<? super T> p : predicates) {
					if (!p.test(t)) {
						return false;
					}
				}
				return true;
			}
		};
	}

	/**
	 * Returns a {@literal Predicate} which negates the given {@literal Predicate}.
	 *
	 * @param p
	 * 		the {@literal Predicate} to negate
	 *
	 * @return A new {@literal Predicate} which is always the opposite of the result of this {@literal Predicate}.
	 */
	public static <T> Predicate<T> negate(final Predicate<? super T> p) {
		return new Predicate<T>() {
			@Override
			public boolean test(T t) {
				return !p.test(t);
			}
		};
	}

	/**
	 * Returns a {@literal Predicate} which evaluates to {@literal true} if either this predicate or the provided
	 * predicate
	 * evaluate to {@literal true}.
	 *
	 * @param p1
	 * 		A {@literal Predicate} which will be ORed together with {@literal p2}.
	 * @param p2
	 * 		A {@literal Predicate} which will be ORed together with {@literal p1}.
	 *
	 * @return A new {@literal Predicate} which returns {@literal true} if either {@literal Predicate} returns {@literal
	 * true}.
	 */
	public static <T> Predicate<T> or(final Predicate<? super T> p1, final Predicate<? super T> p2) {
		Assert.notNull(p1, "Predicate 1 cannot be null.");
		Assert.notNull(p2, "Predicate 2 cannot be null.");
		return new Predicate<T>() {
			@Override
			public boolean test(T t) {
				return p1.test(t) || p2.test(t);
			}
		};
	}

	/**
	 * Returns a {@literal Predicate} which evaluates to {@literal true} if either both {@literal Predicate Predicates}
	 * return {@literal true} or neither of them do.
	 *
	 * @param p1
	 * 		A {@literal Predicate} which will be XORed together with {@literal p2}.
	 * @param p2
	 * 		A {@literal Predicate} which will be XORed together with {@literal p1}.
	 *
	 * @return A new {@literal Predicate} which returns {@literal true} if both {@literal Predicate Predicates} return
	 * {@literal true} or neither of them do.
	 */
	public static <T> Predicate<T> xor(final Predicate<? super T> p1, final Predicate<? super T> p2) {
		Assert.notNull(p1, "Predicate 1 cannot be null.");
		Assert.notNull(p2, "Predicate 2 cannot be null.");
		return new Predicate<T>() {
			@Override
			public boolean test(T t) {
				return (!p1.test(t) && !p2.test(t))
						|| (p1.test(t) && p2.test(t));
			}
		};
	}

}
