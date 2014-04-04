package reactor.event.registry;

import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.block.function.checked.CheckedFunction;
import com.gs.collections.impl.block.predicate.checked.CheckedPredicate;
import com.gs.collections.impl.block.procedure.checked.CheckedProcedure;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.list.mutable.MultiReaderFastList;
import com.gs.collections.impl.multimap.list.SynchronizedPutFastListMultimap;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of {@link reactor.event.registry.Registry} that uses a level 1 cache. New registrations are cached if
 * they are an {@link reactor.event.selector.ObjectSelector} since that means a fast object equality match so the cache
 * can be safely primed. New registrations will also be matched against all the keys in the cache to see if they match
 * any and if so, the new item is added to that cache entry.
 *
 * @author Jon Brisbin
 */
public class CachingRegistry<T> implements Registry<T> {

	private final boolean          useCache;
	private final Consumer<Object> onNotFound;

	private final MultiReaderFastList<Registration<? extends T>>                     registrations;
	private final SynchronizedPutFastListMultimap<Object, Registration<? extends T>> cache;

	public CachingRegistry() {
		this(true, null);
	}

	public CachingRegistry(boolean useCache, Consumer<Object> onNotFound) {
		this.useCache = useCache;
		this.onNotFound = onNotFound;
		this.registrations = MultiReaderFastList.newList();
		this.cache = SynchronizedPutFastListMultimap.newMultimap();
	}

	@Override
	public <V extends T> Registration<V> register(final Selector sel, V obj) {
		final SelectorCacheKeyPredicate selectorPredicate = new SelectorCacheKeyPredicate(sel);

		final AtomicReference<CachableRegistration<V>> ref = new AtomicReference<>();
		final CachableRegistration<V> reg = new CachableRegistration<V>(
				sel,
				obj,
				new Runnable() {
					@Override
					public void run() {
						registrations.remove(ref.get());
						cache.keysView()
						     .select(selectorPredicate)
						     .forEach(new CheckedProcedure<Object>() {
							     @Override
							     public void safeValue(Object key) throws Exception {
								     cache.removeAll(key);
							     }
						     });

					}
				}
		);
		// this little trick is to be able to reference this reg later for cancellation
		ref.set(reg);
		// add to main list
		registrations.withWriteLockAndDelegate(
				new CheckedProcedure<MutableList<Registration<? extends T>>>() {
					@Override
					public void safeValue(MutableList<Registration<? extends T>> regs) throws Exception {
						regs.add(reg);
					}
				}
		);

		if (useCache) {
			if (isDirectMatchable(sel)) {
				cache.put(sel.getObject(), reg);
			} else {
				cache.keysView()
				     .select(selectorPredicate)
				     .forEach(new CheckedProcedure<Object>() {
					     @Override
					     public void safeValue(Object cacheKey) throws Exception {
						     cache.put(cacheKey, reg);
					     }
				     });
			}
		}

		return reg;
	}

	@Override
	public boolean unregister(Object key) {
		final AtomicBoolean changed = new AtomicBoolean(false);
		registrations
				.select(new CacheKeyPredicate<T>(key))
				.forEach(new CheckedProcedure<Registration<? extends T>>() {
					@Override
					public void safeValue(Registration<? extends T> reg) throws Exception {
						changed.set(true);
						reg.cancel();
					}
				});
		return changed.get();
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Registration<? extends T>> select(final Object key) {
		if (null == key) {
			return Collections.emptyList();
		}

		List<Registration<? extends T>> regs;
		if (useCache && !((regs = cache.get(key)).isEmpty())) {
			return regs;
		} else {
			cacheMiss(key);
		}

		regs = registrations
				.select(new CacheKeyPredicate<T>(key))
				.collect(new CheckedFunction<Registration<? extends T>, Registration<? extends T>>() {
					@Override
					public Registration<? extends T> safeValueOf(Registration<? extends T> reg) throws Exception {
						if (useCache) {
							cache.put(key, reg);
						}
						return reg;
					}
				});

		if (regs.isEmpty() && null != onNotFound) {
			onNotFound.accept(key);
		}

		return regs;
	}

	@Override
	public void clear() {
		registrations.clear();
		cache.clear();
	}

	@Override
	public Iterator<Registration<? extends T>> iterator() {
		return FastList.<Registration<? extends T>>newList(registrations).iterator();
	}

	private boolean isDirectMatchable(Selector sel) {
		Class<?> objType = sel.getObject().getClass();
		if (Object.class.equals(objType) || Selectors.AnonymousKey.class.equals(objType)) {
			return true;
		} else {
			return false;
		}
	}

	protected void cacheMiss(Object key) {
	}

	private static class SelectorCacheKeyPredicate extends CheckedPredicate<Object> {
		private final Selector sel;

		private SelectorCacheKeyPredicate(Selector sel) {
			this.sel = sel;
		}

		@Override
		public boolean safeAccept(Object cacheKey) throws Exception {
			return sel.matches(cacheKey);
		}
	}

	private static class CacheKeyPredicate<T> extends CheckedPredicate<Registration<? extends T>> {
		private final Object cacheKey;

		private CacheKeyPredicate(Object cacheKey) {
			this.cacheKey = cacheKey;
		}

		@Override
		public boolean safeAccept(Registration<? extends T> reg) throws Exception {
			return !reg.isCancelled() && reg.getSelector().matches(cacheKey);
		}
	}

}
