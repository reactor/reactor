package reactor.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import reactor.fn.Registration;
import reactor.fn.Registry;
import reactor.fn.SelectionStrategy;
import reactor.fn.Selector;
import reactor.fn.selector.BaseSelector;

/**
 * @author Jon Brisbin
 * @author Andy Wilkinson
 */
public class CachingRegistry<T> implements Registry<T> {

	private final    Random                                     random                = new Random();
	private final    Object                                     monitor               = new Object();
	private final    List<Registration<? extends T>>            registrations         = new ArrayList<Registration<? extends T>>();
	private final    Map<Long, List<Registration<? extends T>>> registrationCache     = new HashMap<Long, List<Registration<? extends T>>>();

	private volatile LoadBalancingStrategy                      loadBalancingStrategy = LoadBalancingStrategy.NONE;
	private          SelectionStrategy                          selectionStrategy;
	private          boolean                                    refreshRequired;

	public LoadBalancingStrategy getLoadBalancingStrategy() {
		return loadBalancingStrategy;
	}

	@Override
	public Registry<T> setLoadBalancingStrategy(LoadBalancingStrategy lb) {
		if (null == lb) {
			lb = LoadBalancingStrategy.NONE;
		}
		this.loadBalancingStrategy = lb;
		return this;
	}

	public SelectionStrategy getSelectionStrategy() {
		synchronized(monitor) {
			return selectionStrategy;
		}
	}

	@Override
	public Registry<T> setSelectionStrategy(SelectionStrategy selectionStrategy) {
		synchronized(monitor) {
			this.selectionStrategy = selectionStrategy;
			refreshRequired = true;
		}
		return this;
	}

	@Override
	public <V extends T> Registration<V> register(Selector sel, V obj) {
		CachableRegistration<V> reg = new CachableRegistration<V>(sel, obj);

		synchronized (monitor) {
			registrations.add(reg);
			refreshRequired = true;
		}

		return reg;
	}

	@Override
	public boolean unregister(Selector sel) {
		synchronized (monitor) {
			if (registrations.isEmpty()) {
				return false;
			}

			List<Registration<? extends T>> regs = findMatchingRegistrations(sel);

			if (!regs.isEmpty()) {
				registrations.removeAll(regs);
				refreshRequired = true;
				return true;
			} else {
				return false;
			}
		}
	}

	@Override
	public Iterable<Registration<? extends T>> select(Selector sel) {
		List<Registration<? extends T>> matchingRegistrations;

		synchronized (monitor) {
			refreshCacheIfNecessary();
			matchingRegistrations = registrationCache.get(sel.getId());
			if (null == matchingRegistrations) {
				matchingRegistrations = find(sel);
			}
		}

		switch (loadBalancingStrategy) {
			case ROUND_ROBIN: {
				if (!(sel instanceof BaseSelector)) {
					throw new IllegalArgumentException("Cannot ROUND_ROBIN load balance using a " + sel.getClass().getName());
				}
				int i = (int) (((BaseSelector<?>) sel).getUsageCount() % matchingRegistrations.size());
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

	@Override
	public Iterator<Registration<? extends T>> iterator() {
		synchronized (monitor) {
			return Collections.unmodifiableList(new ArrayList<Registration<? extends T>>(this.registrations)).iterator();
		}
	}

	private void refreshCacheIfNecessary() {
		synchronized (monitor) {
			if (refreshRequired) {
				registrationCache.clear();
				for (Registration<? extends T> reg : registrations) {
					find(reg.getSelector());
				}
				refreshRequired = false;
			}
		}
	}

	private List<Registration<? extends T>> find(final Selector sel) {
		synchronized (monitor) {
			if (registrations.isEmpty()) {
				return Collections.emptyList();
			}

			List<Registration<? extends T>> regs = findMatchingRegistrations(sel);
			registrationCache.put(sel.getId(), regs);

			return regs;
		}
	}

	private List<Registration<? extends T>> findMatchingRegistrations(Selector sel) {
		List<Registration<? extends T>> regs = new ArrayList<Registration<? extends T>>();
		for (Registration<? extends T> reg : registrations) {
			if (null != selectionStrategy
					&& selectionStrategy.supports(sel)
					&& selectionStrategy.matches(reg.getSelector(), sel)) {
				regs.add(reg);
			} else if (reg.getSelector().matches(sel)) {
				regs.add(reg);
			}
		}
		return regs;
	}

	@SuppressWarnings("unchecked")
	private void expire(Selector sel, long olderThan) {
		synchronized (monitor) {
			List<Registration<? extends T>> regsToRemove = new ArrayList<Registration<? extends T>>();
			for (Registration<? extends T> reg : find(sel)) {
				if (((CachableRegistration<?>) reg).getAge() > olderThan) {
					regsToRemove.add(reg);
				}
			}
			registrations.removeAll(regsToRemove);
			refreshRequired = true;
		}
	}

	private class CachableRegistration<V> implements Registration<V> {
		private final long created = System.currentTimeMillis();
		private final Selector selector;
		private final V        object;
		private volatile boolean cancelAfterUse = false;
		private volatile boolean cancelled      = false;
		private volatile boolean paused         = false;

		private CachableRegistration(Selector selector, V object) {
			this.selector = selector;
			this.object = object;
		}

		long getAge() {
			return System.currentTimeMillis() - created;
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
			registrations.remove(CachableRegistration.this);
			refreshRequired = true;
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
