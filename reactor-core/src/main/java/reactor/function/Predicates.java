package reactor.function;

import reactor.util.Assert;

/**
 * @author Jon Brisbin
 */
public abstract class Predicates {

	protected Predicates() {
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
				for(Predicate<? super T> p : predicates) {
					if(!p.test(t)) {
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
