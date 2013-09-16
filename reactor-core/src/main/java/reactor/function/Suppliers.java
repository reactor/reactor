package reactor.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicInteger;

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
	 * @param obj
	 * 		the object to supply
	 * @param <T>
	 * 		type of the supplied object
	 *
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
	 * @param obj
	 * 		the object to supply
	 * @param <T>
	 * 		type of the supplied object
	 *
	 * @return the new {@link Supplier}
	 */
	@SuppressWarnings("unchecked")
	public static <T> Supplier<T> supplyOnce(final T obj) {
		return drain(Arrays.asList(obj));
	}

	/**
	 * Supply the given object to callers only as long as the given {@link reactor.function.Predicate} returns true.
	 *
	 * @param obj
	 * 		the object to supply
	 * @param predicate
	 * 		the predicate to check to determine whether or not to supply the given value
	 * @param <T>
	 * 		type of the supplied object
	 *
	 * @return the new {@link Supplier}
	 */
	public static <T> Supplier<T> supplyWhile(final T obj, final Predicate<T> predicate) {
		return new Supplier<T>() {
			@Override
			public T get() {
				if(predicate.test(obj)) {
					return obj;
				} else {
					return null;
				}
			}
		};
	}

	/**
	 * Create a {@link reactor.function.Supplier} that continually round-robin load balances each call to {@link
	 * Supplier#get()} by iterating over the objects. When the end is reached, it wraps around to the first object and
	 * keeps providing objects to callers.
	 *
	 * @param objs
	 * 		the objects to load-balance
	 * @param <T>
	 * 		type of the supplied object
	 *
	 * @return the new {@link Supplier}
	 */
	public static <T> Supplier<T> roundRobin(final T... objs) {
		final AtomicInteger count = new AtomicInteger();
		final int len = objs.length;

		return new Supplier<T>() {
			@Override public T get() {
				return objs[count.incrementAndGet() % len];
			}
		};
	}

	/**
	 * Filter the given {@link Iterable} using the given {@link Predicate} so that calls to the return {@link
	 * reactor.function.Supplier#get()} will provide only items from the original collection which pass the predicate
	 * test.
	 *
	 * @param src
	 * 		the source of objects to filter
	 * @param predicate
	 * 		the {@link Predicate} to test items against
	 * @param <T>
	 * 		type of the source
	 *
	 * @return the new {@link Supplier}
	 */
	public static <T> Supplier<T> filter(final Iterable<T> src, final Predicate<T> predicate) {
		return new Supplier<T>() {
			Iterator<T> iter = src.iterator();

			@Override
			public T get() {
				if(!iter.hasNext()) {
					return null;
				}
				T obj;
				do {
					obj = iter.next();
				} while(!predicate.test(obj));
				return obj;
			}
		};
	}

	/**
	 * Create a {@link reactor.function.Supplier} which drains the contents of the given {@link java.lang.Iterable} by
	 * internally creating an {@link java.util.Iterator} and delegating each call of {@link
	 * reactor.function.Supplier#get()} to {@link java.util.Iterator#next()}.
	 *
	 * @param c
	 * 		the collection to drain
	 * @param <T>
	 * 		type of the source
	 *
	 * @return the new {@link Supplier}
	 */
	public static <T> Supplier<T> drain(Iterable<T> c) {
		final Iterator<T> iter = c.iterator();

		return new Supplier<T>() {
			@Override
			public T get() {
				return (iter.hasNext() ? iter.next() : null);
			}
		};
	}

	/**
	 * Create a {@link reactor.function.Supplier} which drains all of the given {@link java.lang.Iterable Iterables}.
	 *
	 * @param iters
	 * 		the collections to drain
	 * @param <T>
	 * 		type of the source
	 *
	 * @return the new {@link Supplier}
	 *
	 * @see #drain(Iterable)
	 */
	public static <T> Supplier<T> drainAll(Iterable<Iterable<T>> iters) {
		List<Supplier<T>> ls = new ArrayList<Supplier<T>>();
		for(Iterable<T> iter : iters) {
			ls.add(drain(iter));
		}
		return collect(ls);
	}

	/**
	 * Create a {@link reactor.function.Supplier} that aggregates the given list of suppliers by calling each one, in
	 * turn, until the supplier returns {@code null}. The aggregator then goes on to the next supplier in the list and
	 * delegates calls to that supplier, and so on, until the end of the list is reached.
	 *
	 * @param suppliers
	 * 		the list of suppliers to delegate to
	 * @param <T>
	 * 		type of the source
	 *
	 * @return the new {@link Supplier}
	 */
	public static <T> Supplier<T> collect(List<Supplier<T>> suppliers) {
		final ListIterator<Supplier<T>> iter = suppliers.listIterator();

		return new Supplier<T>() {
			@Override
			public synchronized T get() {
				if(iter.hasNext()) {
					T obj = iter.next().get();
					if(null != obj) {
						return obj;
					}
				} else if(iter.hasPrevious()) {
					// rewind
					while(iter.hasPrevious()) {
						iter.previous();
					}
					return get();
				}
				return null;
			}
		};
	}

}
