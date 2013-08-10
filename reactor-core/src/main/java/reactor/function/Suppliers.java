package reactor.function;

import java.util.*;

/**
 * Helper class for working tying {@link Supplier Suppliers} to {@link Iterable Iterables} and other types of
 * collections.
 *
 * @author Jon Brisbin
 */
public abstract class Suppliers {

	private Suppliers() {
	}

	/**
	 * Wrap the given object that will supply the given object every time {@link reactor.function.Supplier#get()} is
	 * called.
	 *
	 * @param obj the object to supply
	 * @param <T> type of the supplied object
	 * @return the new {@link Supplier}
	 */
	public static <T> Supplier<T> supply(final T obj) {
		return new Supplier<T>() {
			@Override
			public T get() {
				return obj;
			}
		};
	}

	/**
	 * Supply the given object only once, the first time {@link reactor.function.Supplier#get()} is invoked.
	 *
	 * @param obj the object to supply
	 * @param <T> type of the supplied object
	 * @return the new {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<T> supplyOnce(final T obj) {
		return drain(Arrays.asList(obj));
	}

	public static <T> Supplier<T> supplyWhile(final T obj, final Predicate<T> predicate) {
		return new Supplier<T>() {
			@Override
			public T get() {
				if (predicate.test(obj)) {
					return obj;
				} else {
					return null;
				}
			}
		};
	}

	/**
	 * Filter the given {@link Iterable} using the given {@link Predicate} so that calls to the return {@link
	 * reactor.function.Supplier#get()} will provide only items from the original collection which pass the predicate
	 * test.
	 *
	 * @param src       the source of objects to filter
	 * @param predicate the {@link Predicate} to test items against
	 * @param <T>       type of the source
	 * @return the new {@link Supplier}
	 */
	public static <T> Supplier<T> filter(final Iterable<T> src, final Predicate<T> predicate) {
		return new Supplier<T>() {
			Iterator<T> iter = src.iterator();

			@Override
			public T get() {
				if (!iter.hasNext()) {
					return null;
				}
				T obj;
				do {
					obj = iter.next();
				} while (!predicate.test(obj));
				return obj;
			}
		};
	}

	public static <T> Supplier<T> drain(Iterable<T> c) {
		final Iterator<T> iter = c.iterator();

		return new Supplier<T>() {
			@Override
			public T get() {
				return (iter.hasNext() ? iter.next() : null);
			}
		};
	}

	public static <T> Supplier<T> drainAll(Iterable<Iterable<T>> iters) {
		List<Supplier<T>> ls = new ArrayList<Supplier<T>>();
		for (Iterable<T> iter : iters) {
			ls.add(drain(iter));
		}
		return collect(ls);
	}

	public static <T> Supplier<T> collect(List<Supplier<T>> suppliers) {
		final ListIterator<Supplier<T>> iter = suppliers.listIterator();

		return new Supplier<T>() {
			@Override
			public synchronized T get() {
				if (iter.hasNext()) {
					T obj = iter.next().get();
					if (null != obj) {
						return obj;
					}
				} else if (iter.hasPrevious()) {
					// rewind
					while (iter.hasPrevious()) {
						iter.previous();
					}
					return get();
				}
				return null;
			}
		};
	}

}
