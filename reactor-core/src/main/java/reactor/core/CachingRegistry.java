/*
 * Copyright (c) 2011-2013 the original author or authors.
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

package reactor.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.fn.Registration;
import reactor.fn.Registry;
import reactor.fn.SelectionStrategy;
import reactor.fn.Selector;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An optimized selectors registry working with a L1 Cache and ReadWrite reentrant locks
 * Events dispatching strategy can be tuned through its {@link LoadBalancingStrategy} .
 *
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public class CachingRegistry<T> implements Registry<T> {

	private final Random                                       random            = new Random();
	private final ReentrantReadWriteLock                       readWriteLock     = new ReentrantReadWriteLock(true);
	private final Lock                                         readLock          = readWriteLock.readLock();
	private final Lock                                         writeLock         = readWriteLock.writeLock();
	private final List<Registration<? extends T>>              registrations     = new ArrayList<Registration<? extends T>>();
	private final Map<Object, List<Registration<? extends T>>> registrationCache = new HashMap<Object, List<Registration<? extends T>>>();
	private final Map<Object, AtomicLong>                      usageCounts       = new HashMap<Object, AtomicLong>();

	private final LoadBalancingStrategy loadBalancingStrategy;
	private final SelectionStrategy     selectionStrategy;

	private boolean refreshRequired;

	public CachingRegistry(LoadBalancingStrategy loadBalancingStrategy, SelectionStrategy selectionStrategy) {
		this.loadBalancingStrategy = loadBalancingStrategy == null ? LoadBalancingStrategy.NONE : loadBalancingStrategy;
		this.selectionStrategy = selectionStrategy;
	}

	public SelectionStrategy getSelectionStrategy() {
		return selectionStrategy;
	}

	public LoadBalancingStrategy getLoadBalancingStrategy() {
		return loadBalancingStrategy;
	}

	@Override
	public <V extends T> Registration<V> register(Selector sel, V obj) {
		CachableRegistration<V> reg = new CachableRegistration<V>(sel, obj);

		writeLock.lock();
		try {
			registrations.add(reg);
			refreshRequired = true;
		} finally {
			writeLock.unlock();
		}

		return reg;
	}

	@Override
	public boolean unregister(Object key) {

		writeLock.lock();
		try {
			if (registrations.isEmpty()) {
				return false;
			}

			List<Registration<? extends T>> regs = findMatchingRegistrations(key);

			if (!regs.isEmpty()) {
				registrations.removeAll(regs);
				refreshRequired = true;
				return true;
			} else {
				return false;
			}
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	public Iterable<Registration<? extends T>> select(Object key) {
		List<Registration<? extends T>> matchingRegistrations;

		readLock.lock();

		try {
			if (refreshRequired) {
				readLock.unlock();
				writeLock.lock();
				try {
					if (refreshRequired) {
						registrationCache.clear();
						refreshRequired = false;
					}
				} finally {
					readLock.lock();
					writeLock.unlock();
				}
			}

			matchingRegistrations = registrationCache.get(key);

			if (null == matchingRegistrations) {
				readLock.unlock();
				writeLock.lock();
				try {
					matchingRegistrations = registrationCache.get(key);
					if (null == matchingRegistrations) {
						matchingRegistrations = find(key);
					}
				} finally {
					readLock.lock();
					writeLock.unlock();
				}
			}
		} finally {
			readLock.unlock();
		}

		switch (loadBalancingStrategy) {
			case ROUND_ROBIN: {
				int i = (int) (getUsageCount(key).incrementAndGet() % matchingRegistrations.size());
				return Collections.<Registration<? extends T>>singletonList(matchingRegistrations.get(i));
			}
			case RANDOM: {
				int i = random.nextInt(matchingRegistrations.size());
				return Collections.<Registration<? extends T>>singletonList(matchingRegistrations.get(i));
			}
			default:
				return matchingRegistrations;
		}
	}

	private AtomicLong getUsageCount(Object key) {
		readLock.lock();
		try {
			AtomicLong usageCount = this.usageCounts.get(key);
			if (usageCount == null) {
				readLock.unlock();
				writeLock.lock();
				try {
					if (usageCount == null) {
						usageCount = new AtomicLong();
						this.usageCounts.put(key, usageCount);
					}
				} finally {
					writeLock.unlock();
					readLock.lock();
				}
			}
			return usageCount;
		} finally {
			readLock.unlock();
		}
	}

	@Override
	public Iterator<Registration<? extends T>> iterator() {
		try {
			readLock.lock();
			return Collections.unmodifiableList(new ArrayList<Registration<? extends T>>(this.registrations)).iterator();
		} finally {
			readLock.unlock();
		}
	}

	private List<Registration<? extends T>> find(Object object) {
		cacheMiss(object);
		try {
			writeLock.lock();

			List<Registration<? extends T>> regs;

			if (registrations.isEmpty()) {
				regs = Collections.emptyList();
			} else {
				regs = findMatchingRegistrations(object);
			}

			registrationCache.put(object, regs);

			return regs;
		} finally {
			writeLock.unlock();
		}
	}

	private List<Registration<? extends T>> findMatchingRegistrations(Object object) {
		List<Registration<? extends T>> regs = new ArrayList<Registration<? extends T>>();
		for (Registration<? extends T> reg : registrations) {
			if (null != selectionStrategy
					&& selectionStrategy.supports(object)
					&& selectionStrategy.matches(reg.getSelector(), object)) {
				regs.add(reg);
			} else if (reg.getSelector().matches(object)) {
				regs.add(reg);
			}
		}
		if (regs.isEmpty()) {
			Logger log = LoggerFactory.getLogger(CachingRegistry.class);
			if (log.isTraceEnabled()) {
				log.trace("No objects registered for key {}", object);
			}
		}
		return regs;
	}

	protected void cacheMiss(Object key) {

	}

	private class CachableRegistration<V> implements Registration<V> {
		private final Selector selector;
		private final V        object;
		private volatile boolean cancelAfterUse = false;
		private volatile boolean cancelled      = false;
		private volatile boolean paused         = false;

		private CachableRegistration(Selector selector, V object) {
			this.selector = selector;
			this.object = object;
		}

		@Override
		public Selector getSelector() {
			return selector;
		}

		@Override
		public V getObject() {
			return object;
		}

		@Override
		public Registration<V> cancelAfterUse() {
			cancelAfterUse = !cancelAfterUse;
			return this;
		}

		@Override
		public boolean isCancelAfterUse() {
			return cancelAfterUse;
		}

		@Override
		public Registration<V> cancel() {
			this.cancelled = true;

			writeLock.lock();
			try {
				registrations.remove(CachableRegistration.this);
				refreshRequired = true;
			} finally {
				writeLock.unlock();
			}

			return this;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public Registration<V> pause() {
			paused = true;
			return this;
		}

		@Override
		public boolean isPaused() {
			return paused;
		}

		@Override
		public Registration<V> resume() {
			paused = false;
			return this;
		}
	}

}
