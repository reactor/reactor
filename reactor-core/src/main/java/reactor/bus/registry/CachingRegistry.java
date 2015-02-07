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

package reactor.bus.registry;

import com.gs.collections.api.block.procedure.Procedure;
import com.gs.collections.api.list.MutableList;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.list.mutable.MultiReaderFastList;
import com.gs.collections.impl.map.mutable.UnifiedMap;
import reactor.bus.selector.Selector;
import reactor.fn.Consumer;
import reactor.jarjar.jsr166e.ConcurrentHashMapV8;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link Registry} that uses a partitioned cache that partitions on thread
 * id.
 *
 * @author Jon Brisbin
 */
public class CachingRegistry<T> implements Registry<T> {

	private final NewThreadLocalRegsFn newThreadLocalRegsFn = new NewThreadLocalRegsFn();

	private final boolean                                                                        useCache;
	private final boolean                                                                        cacheNotFound;
	private final Consumer<Object>                                                               onNotFound;
	private final MultiReaderFastList<Registration<? extends T>>                                 registrations;
	private final ConcurrentHashMapV8<Long, UnifiedMap<Object, List<Registration<? extends T>>>> threadLocalCache;

	 CachingRegistry(boolean useCache, boolean cacheNotFound, Consumer<Object> onNotFound) {
		this.useCache = useCache;
		this.cacheNotFound = cacheNotFound;
		this.onNotFound = onNotFound;
		this.registrations = MultiReaderFastList.newList();
		this.threadLocalCache = new ConcurrentHashMapV8<Long, UnifiedMap<Object, List<Registration<? extends T>>>>();
	}

	@Override
	public Registration<T> register(Selector sel, T obj) {
		RemoveRegistration removeFn = new RemoveRegistration();
		final Registration<T> reg = new CachableRegistration<>(sel, obj, removeFn);
		removeFn.reg = reg;

		registrations.withWriteLockAndDelegate(new Procedure<MutableList<Registration<? extends T>>>() {
			@Override
			public void value(MutableList<Registration<? extends T>> regs) {
				regs.add(reg);
			}
		});
		if (useCache) {
			threadLocalCache.clear();
		}

		return reg;
	}

	@Override
	@SuppressWarnings("unchecked")
	public boolean unregister(final Object key) {
		final AtomicBoolean modified = new AtomicBoolean(false);
		registrations.withWriteLockAndDelegate(new Procedure<MutableList<Registration<? extends T>>>() {
			@Override
			public void value(final MutableList<Registration<? extends T>> regs) {
				Iterator<Registration<? extends T>> registrationIterator = regs.iterator();
				Registration<? extends T> reg;
				while (registrationIterator.hasNext()) {
					reg = registrationIterator.next();
					if (reg.getSelector().matches(key)) {
						registrationIterator.remove();
						modified.compareAndSet(false, true);
					}
				}
				if (useCache && modified.get()) {
					threadLocalCache.clear();
				}
			}
		});
		return modified.get();
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<Registration<? extends T>> select(Object key) {
		// use a thread-local cache
		UnifiedMap<Object, List<Registration<? extends T>>> allRegs = threadLocalRegs();

		// maybe pull Registrations from cache for this key
		List<Registration<? extends T>> selectedRegs = null;
		if (useCache && (null != (selectedRegs = allRegs.get(key)))) {
			return selectedRegs;
		}

		// cache not used or cache miss
		cacheMiss(key);
		selectedRegs = FastList.newList();

		// find Registrations based on Selector
		for (Registration<? extends T> reg : this) {
			if (reg.getSelector().matches(key)) {
				selectedRegs.add(reg);
			}
		}
		if (useCache && (!selectedRegs.isEmpty() || cacheNotFound)) {
			allRegs.put(key, selectedRegs);
		}

		// nothing found, maybe invoke handler
		if (selectedRegs.isEmpty() && (null != onNotFound)) {
			onNotFound.accept(key);
		}

		// return
		return selectedRegs;
	}

	@Override
	public void clear() {
		registrations.clear();
		threadLocalCache.clear();
	}

	@Override
	public Iterator<Registration<? extends T>> iterator() {
		return FastList.newList(registrations).iterator();
	}

	protected void cacheMiss(Object key) {
	}

	private UnifiedMap<Object, List<Registration<? extends T>>> threadLocalRegs() {
		Long threadId = Thread.currentThread().getId();
		UnifiedMap<Object, List<Registration<? extends T>>> regs;
		if (null == (regs = threadLocalCache.get(threadId))) {
			regs = threadLocalCache.computeIfAbsent(threadId, newThreadLocalRegsFn);
		}
		return regs;
	}

	private final class RemoveRegistration implements Runnable {
		Registration<? extends T> reg;

		@Override
		public void run() {
			registrations.withWriteLockAndDelegate(new Procedure<MutableList<Registration<? extends T>>>() {
				@Override
				public void value(MutableList<Registration<? extends T>> regs) {
					regs.remove(reg);
					threadLocalCache.clear();
				}
			});
		}
	}

	private final class NewThreadLocalRegsFn
			implements ConcurrentHashMapV8.Fun<Long, UnifiedMap<Object, List<Registration<? extends T>>>> {
		@Override
		public UnifiedMap<Object, List<Registration<? extends T>>> apply(Long aLong) {
			return UnifiedMap.newMap();
		}
	}

}
