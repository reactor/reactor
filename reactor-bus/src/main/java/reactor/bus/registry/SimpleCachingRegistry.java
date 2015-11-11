/*
 * Copyright (c) 2011-2016 Pivotal Software, Inc.
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

package reactor.bus.registry;

import reactor.bus.selector.Selector;
import reactor.fn.Consumer;
import reactor.jarjar.jsr166e.ConcurrentHashMapV8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A naive caching Registry implementation for use in situations that the default {@code CachingRegistry} can't be used
 * due to its reliance on the gs-collections library. Not designed for high throughput but for use on things like
 * handheld devices, where the synchronized nature of the registry won't affect performance much.
 */
public class SimpleCachingRegistry<K, V> implements Registry<K, V> {

	private final ConcurrentHashMapV8<Object, List<Registration<K, ? extends V>>>      cache         = new
	  ConcurrentHashMapV8<>();
	private final ConcurrentHashMapV8<Selector<K>, List<Registration<K, ? extends V>>> registrations = new
	  ConcurrentHashMapV8<>();

	private final boolean     useCache;
	private final boolean     cacheNotFound;
	private final Consumer<K> onNotFound;

	SimpleCachingRegistry(boolean useCache, boolean cacheNotFound, Consumer<K> onNotFound) {
		this.useCache = useCache;
		this.cacheNotFound = cacheNotFound;
		this.onNotFound = onNotFound;
	}

	@Override
	public synchronized Registration<K, V> register(final Selector<K> sel, V obj) {
		List<Registration<K, ? extends V>> regs;
		if (null == (regs = registrations.get(sel))) {
			regs = registrations.computeIfAbsent(sel,
			  new ConcurrentHashMapV8.Fun<Selector<K>, List<Registration<K, ? extends V>>>() {
				  @Override
				  public List<Registration<K, ? extends V>> apply(Selector<K> selector) {
					  return new ArrayList<Registration<K, ? extends V>>();
				  }
			  });
		}

		Registration<K, V> reg = new CachableRegistration<>(sel, obj, new Runnable() {
			@Override
			public void run() {
				registrations.remove(sel);
				cache.clear();
			}
		});
		regs.add(reg);

		return reg;
	}

	@Override
	@SuppressWarnings("unchecked")
	public synchronized boolean unregister(Object key) {
		boolean found = false;
		for (Selector sel : registrations.keySet()) {
			if (!sel.matches(key)) {
				continue;
			}
			if (null != registrations.remove(sel) && !found) {
				found = true;
			}
		}
		if (useCache)
			cache.remove(key);
		return found;
	}

	@Override
	@SuppressWarnings("unchecked")
	public synchronized List<Registration<K, ? extends V>> select(final K key) {
		List<Registration<K, ? extends V>> selectedRegs;
		if (null != (selectedRegs = cache.get(key))) {
			return selectedRegs;
		}

		final List<Registration<K, ? extends V>> regs = new ArrayList<Registration<K, ? extends V>>();
		registrations.forEach(new ConcurrentHashMapV8.BiAction<Selector<K>, List<Registration<K, ? extends V>>>() {
			@Override
			public void apply(Selector<K> selector, List<Registration<K, ? extends V>> registrations) {
				if (selector.matches(key)) {
					regs.addAll(registrations);
				}
			}
		});

		if (regs.isEmpty() && null != onNotFound) {
			onNotFound.accept(key);
		}
		if (useCache && (!regs.isEmpty() || cacheNotFound)) {
			cache.put(key, regs);
		}

		return regs;
	}

	@SuppressWarnings("unchecked")
	public static <K, V> Iterable<? extends V> selectValues(Registry<K, V> reg, final K key) {
		final List<Registration<K, ? extends V>> registrations = reg.select(key);

		if(registrations.isEmpty()){
			return Collections.EMPTY_LIST;
		}

		return new Iterable<V>() {
			@Override
			public Iterator<V> iterator() {
				final Iterator<Registration<K, ? extends V>> it = registrations.iterator();
				return new Iterator<V>() {
					@Override
					public boolean hasNext() {
						return it.hasNext();
					}

					@Override
					public V next() {
						return it.next().getObject();
					}
				};
			}
		};
	}

	@Override
	public Iterable<? extends V> selectValues(final K key) {
		return selectValues(this, key);
	}

	@Override
	public synchronized void clear() {
		cache.clear();
		registrations.clear();
	}

	@Override
	public synchronized Iterator<Registration<K, ? extends V>> iterator() {
		final List<Registration<K, ? extends V>> regs = new ArrayList<Registration<K, ? extends V>>();
		registrations.forEach(new ConcurrentHashMapV8.BiAction<Selector<K>, List<Registration<K, ? extends V>>>() {
			@Override
			public void apply(Selector<K> selector, List<Registration<K, ? extends V>> registrations) {
				regs.addAll(registrations);
			}
		});
		return regs.iterator();
	}

}
