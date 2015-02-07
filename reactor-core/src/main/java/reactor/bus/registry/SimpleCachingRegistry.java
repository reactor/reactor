/*
 * Copyright (c) 2011-2015 Pivotal Software, Inc.
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
import java.util.Iterator;
import java.util.List;

/**
 * A naive caching Registry implementation for use in situations that the default {@code CachingRegistry} can't be used
 * due to its reliance on the gs-collections library. Not designed for high throughput but for use on things like
 * handheld devices, where the synchronized nature of the registry won't affect performance much.
 */
public class SimpleCachingRegistry<T> implements Registry<T> {

	private final ConcurrentHashMapV8<Object, List<Registration<? extends T>>>   cache         = new ConcurrentHashMapV8<Object, List<Registration<? extends T>>>();
	private final ConcurrentHashMapV8<Selector, List<Registration<? extends T>>> registrations = new ConcurrentHashMapV8<Selector, List<Registration<? extends T>>>();

	private final boolean          useCache;
	private final boolean          cacheNotFound;
	private final Consumer<Object> onNotFound;

	SimpleCachingRegistry(boolean useCache, boolean cacheNotFound, Consumer<Object> onNotFound) {
		this.useCache = useCache;
		this.cacheNotFound = cacheNotFound;
		this.onNotFound = onNotFound;
	}

	@Override
	public synchronized Registration<T> register(final Selector sel, T obj) {
		List<Registration<? extends T>> regs;
		if (null == (regs = registrations.get(sel))) {
			regs = registrations.computeIfAbsent(sel,
			                                     new ConcurrentHashMapV8.Fun<Selector, List<Registration<? extends T>>>() {
				                                     @Override
				                                     public List<Registration<? extends T>> apply(Selector selector) {
					                                     return new ArrayList<Registration<? extends T>>();
				                                     }
			                                     });
		}

		Registration<T> reg = new CachableRegistration<>(sel, obj, new Runnable() {
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
	public synchronized List<Registration<? extends T>> select(final Object key) {
		List<Registration<? extends T>> selectedRegs;
		if (null != (selectedRegs = cache.get(key))) {
			return selectedRegs;
		}

		final List<Registration<? extends T>> regs = new ArrayList<Registration<? extends T>>();
		registrations.forEach(new ConcurrentHashMapV8.BiAction<Selector, List<Registration<? extends T>>>() {
			@Override
			public void apply(Selector selector, List<Registration<? extends T>> registrations) {
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

	@Override
	public synchronized void clear() {
		cache.clear();
		registrations.clear();
	}

	@Override
	public synchronized Iterator<Registration<? extends T>> iterator() {
		final List<Registration<? extends T>> regs = new ArrayList<Registration<? extends T>>();
		registrations.forEach(new ConcurrentHashMapV8.BiAction<Selector, List<Registration<? extends T>>>() {
			@Override
			public void apply(Selector selector, List<Registration<? extends T>> registrations) {
				regs.addAll(registrations);
			}
		});
		return regs.iterator();
	}

}
