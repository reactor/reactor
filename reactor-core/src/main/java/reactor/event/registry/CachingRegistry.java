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

package reactor.event.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.event.lifecycle.Lifecycle;
import reactor.event.selector.ObjectSelector;
import reactor.event.selector.Selector;

import java.util.*;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An optimized selectors registry working with a L1 Cache and spare use of reentrant locks.
 *
 * @param <T>
 * 		the type of Registration held by this registry
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class CachingRegistry<T> implements Registry<T> {

	private static final Logger                                     LOG                 = LoggerFactory.getLogger(
			CachingRegistry
					.class);
	private static final Selector                                   NO_MATCH            = new ObjectSelector<Void>(
			null) {
		@Override
		public boolean matches(Object key) {
			return false;
		}
	};
	private static final AtomicIntegerFieldUpdater<CachingRegistry> sizeExponentUpdater = AtomicIntegerFieldUpdater
			.newUpdater(CachingRegistry.class, "sizeExp");
	private static final AtomicIntegerFieldUpdater<CachingRegistry> nextAvailUpdater    = AtomicIntegerFieldUpdater
			.newUpdater(CachingRegistry.class, "nextAvail");

	@SuppressWarnings("unchecked")
	private final Registration<? extends T>[] empty = new Registration[0];

	private final ReentrantLock                             cacheLock = new ReentrantLock();
	private final ReentrantLock                             regLock   = new ReentrantLock();
	private final Map<Integer, Registration<? extends T>[]> cache     = new HashMap<Integer,
			Registration<? extends T>[]>();

	private volatile int nextAvail = 0;
	private volatile Registration<? extends T>[] registrations;

	@SuppressWarnings("unchecked")
	public CachingRegistry() {
		this.registrations = new Registration[32];
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V extends T> Registration<V> register(Selector sel, V obj) {
		int nextIdx = nextAvailUpdater.getAndIncrement(this);
		Registration<? extends T> reg = registrations[nextIdx] = new SimpleRegistration<V>(sel, obj);

		cacheLock.lock();
		try {
			// prime cache for anonymous Objects, Strings, etc...in an ObjectSelector
			if(ObjectSelector.class.equals(sel.getClass())) {
				int hashCode = sel.getObject().hashCode();
				Registration<? extends T>[] regs = cache.get(hashCode);
				if(null == regs) {
					regs = new Registration[]{reg};
				} else {
					regs = addToArray(reg, regs);
				}
				cache.put(hashCode, regs);
			}
		} finally {
			cacheLock.unlock();
		}

		if(nextIdx > registrations.length * .75) {
			regLock.lock();
			try {
				growRegistrationArray();
			} finally {
				regLock.unlock();
			}
		}

		return (Registration<V>)reg;
	}

	@Override
	public boolean unregister(Object key) {
		boolean updated = false;
		regLock.lock();
		try {
			cacheLock.lock();
			try {
				for(Registration<? extends T> reg : select(key)) {
					reg.cancel();
					updated = true;
				}
				cache.remove(key.hashCode());
				return updated;
			} finally {
				cacheLock.unlock();
			}
		} finally {
			regLock.unlock();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<Registration<? extends T>> select(Object key) {
		if(null == key) {
			return Collections.emptyList();
		}
		int hashCode = key.hashCode();
		Registration<? extends T>[] regs = cache.get(hashCode);
		if(null != regs) {
			if(regs == empty) {
				return Collections.emptyList();
			} else {
				return Arrays.asList(regs);
			}
		}

		// cache miss
		cacheMiss(key);
		cacheLock.lock();
		try {
			regs = new Registration[1];
			int found = 0;
			for(int i = 0; i < nextAvail; i++) {
				Registration<? extends T> reg = registrations[i];
				if(null == reg) {
					break;
				}
				if(!reg.isCancelled() && !reg.isPaused() && reg.getSelector().matches(key)) {
					regs = addToArray(reg, regs);
					found++;
				}
			}
			if(found > 0) {
				cache.put(hashCode, regs);
			}
		} finally {
			cacheLock.unlock();
		}

		regs = cache.get(hashCode);
		if(null == regs) {
			// none found
			if(LOG.isTraceEnabled()) {
				LOG.trace("No Registrations found that match " + key);
			}
			regs = empty;
			cacheLock.lock();
			try {
				cache.put(hashCode, regs);
			} finally {
				cacheLock.unlock();
			}
			return Collections.emptyList();
		}
		return Arrays.asList(regs);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterator<Registration<? extends T>> iterator() {
		return Arrays.asList(registrations).iterator();
	}

	protected void cacheMiss(Object key) {}

	@SuppressWarnings("unchecked")
	private Registration<? extends T>[] addToArray(Registration<? extends T> reg,
	                                               Registration<? extends T>[] regs) {
		int len = regs.length;
		for(int i = 0; i < len; i++) {
			if(null == regs[i]) {
				regs[i] = reg;
				return regs;
			}
		}

		// no empty slots, grow the array
		Registration<? extends T>[] newRegs = Arrays.copyOf(regs, len + 1);
		newRegs[len] = reg;

		return newRegs;
	}

	@SuppressWarnings("unchecked")
	private void growRegistrationArray() {
		int newSize = (int)Math.pow(2, sizeExponentUpdater.getAndIncrement(this));
		Registration<? extends T>[] newRegistrations = new Registration[newSize];
		int i = 0;
		for(Registration<? extends T> reg : registrations) {
			if(null == reg) {
				break;
			}
			if(!reg.isCancelled()) {
				newRegistrations[i++] = reg;
			}
		}
		registrations = newRegistrations;
		nextAvailUpdater.set(this, i);
	}

	private class SimpleRegistration<T> implements Registration<T> {
		private final Selector       selector;
		private final T              object;
		private final boolean        lifecycle;

		private volatile boolean cancelled      = false;
		private volatile boolean cancelAfterUse = false;
		private volatile boolean paused         = false;

		private SimpleRegistration(Selector selector, T object) {
			this.selector = selector;
			this.object = object;
			this.lifecycle = Lifecycle.class.isAssignableFrom(object.getClass());
		}

		@Override
		public Selector getSelector() {
			return (!cancelled ? selector : NO_MATCH);
		}

		@Override
		public T getObject() {
			return (!cancelled && !paused ? object : null);
		}

		@Override
		public Registration<T> cancelAfterUse() {
			this.cancelAfterUse = true;
			return this;
		}

		@Override
		public boolean isCancelAfterUse() {
			return cancelAfterUse;
		}

		@Override
		public Registration<T> cancel() {
			if(!cancelled) {
				if(lifecycle){
					((Lifecycle)object).cancel();
				}
				this.cancelled = true;
			}
			return this;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public Registration<T> pause() {
			this.paused = true;
			if(lifecycle){
				((Lifecycle)object).pause();
			}
			return this;
		}

		@Override
		public boolean isPaused() {
			return paused;
		}

		@Override
		public Registration<T> resume() {
			paused = false;
			if(lifecycle){
				((Lifecycle)object).resume();
			}
			return this;
		}
	}

}
