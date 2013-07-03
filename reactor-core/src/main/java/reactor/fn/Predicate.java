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

package reactor.fn;

import reactor.util.Assert;

/**
 * Determines if the input object matches some criteria.
 *
 * @param <T> the type of object that the predicate can test
 *
 * @author Jon Brisbin
 */
public abstract class Predicate<T> {

	/**
	 * Returns a {@literal Predicate} which evaluates to {@literal true} only if this predicate and the provided predicate
	 * both evaluate to {@literal true}.
	 *
	 * @param p A {@literal Predicate} which will be ANDed together with this {@literal Predicate}.
	 * @return A new {@literal Predicate} which returns {@literal true} only if both {@literal Predicate Predicates} return
	 *         {@literal true}.
	 */
	public Predicate<T> and(final Predicate<? super T> p) {
		Assert.notNull(p, "Predicate cannot be null.");
		return new Predicate<T>() {
			@Override
			public boolean test(T t) {
				return Predicate.this.test(t) && p.test(t);
			}
		};
	}

	/**
	 * Returns a {@literal Predicate} which negates this {@literal Predicate}.
	 *
	 * @return A new {@literal Predicate} which is always the opposite of the result of this {@literal Predicate}.
	 */
	public Predicate<T> negate() {
		return new Predicate<T>() {
			@Override
			public boolean test(T t) {
				return !Predicate.this.test(t);
			}
		};
	}

	/**
	 * Returns a {@literal Predicate} which evaluates to {@literal true} if either this predicate or the provided predicate
	 * evaluate to {@literal true}.
	 *
	 * @param p A {@literal Predicate} which will be ORed together with this {@literal Predicate}.
	 * @return A new {@literal Predicate} which returns {@literal true} if either {@literal Predicate} returns {@literal
	 *         true}.
	 */
	public Predicate<T> or(final Predicate<? super T> p) {
		Assert.notNull(p, "Predicate cannot be null.");
		return new Predicate<T>() {
			@Override
			public boolean test(T t) {
				return Predicate.this.test(t) || p.test(t);
			}
		};
	}

	/**
	 * Returns a {@literal Predicate} which evaluates to {@literal true} if either both {@literal Predicate Predicates}
	 * return {@literal true} or neither of them do.
	 *
	 * @param p A {@literal Predicate} which will be XORed together with this {@literal Predicate}.
	 * @return A new {@literal Predicate} which returns {@literal true} if both {@literal Predicate Predicates} return
	 *         {@literal true} or neither of them do.
	 */
	public Predicate<T> xor(final Predicate<? super T> p) {
		Assert.notNull(p, "Predicate cannot be null.");
		return new Predicate<T>() {
			@Override
			public boolean test(T t) {
				return (!Predicate.this.test(t) && !p.test(t))
						|| (Predicate.this.test(t) && p.test(t));
			}
		};
	}

	/**
	 * Returns {@literal true} if the input object matches some criteria.
	 *
	 * @param t The input object.
	 * @return {@literal true} if the criteria matches, {@literal false} otherwise.
	 */
	public abstract boolean test(T t);

}
