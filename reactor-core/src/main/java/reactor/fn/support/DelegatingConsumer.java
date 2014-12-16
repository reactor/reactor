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

package reactor.fn.support;

import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.batch.BatchConsumer;

import javax.annotation.Nonnull;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * An implementation of {@link Consumer} that maintains a list of delegates to which events received by this {@link
 * Consumer} will be passed along.
 * <p/>
 * NOTE: Access to the list of delegates is {@code synchronized} to make it thread-safe, so using this implementation of
 * {@link Consumer} will incur an overall performance hit on throughput. Also, references to the {@link Consumer
 * Consumers} are weak. It's not possible to remove a {@link Consumer} without a reference to it, so the {@code
 * DelegatingConsumer} assumes references to {@link Consumer Consumers} added here have other components with strong
 * references to them somewhere else.
 *
 * @author Jon Brisbin
 */
public class DelegatingConsumer<T> implements BatchConsumer<T>, Iterable<Consumer<T>> {

	private final Object                           delegateMonitor = new Object() {
	};
	private final List<WeakReference<Consumer<T>>> delegates       = new ArrayList<WeakReference<Consumer<T>>>();
	private volatile int delegateSize;

	/**
	 * Add the given {@link Consumer} to the list of delegates.
	 *
	 * @param consumer the {@link Consumer} to add
	 * @return {@literal this}
	 */
	public DelegatingConsumer<T> add(Consumer<T> consumer) {
		Assert.notNull(consumer, "Consumer cannot be null.");
		synchronized (delegateMonitor) {
			delegates.add(new WeakReference<Consumer<T>>(consumer));
			delegateSize = delegates.size();
		}
		return this;
	}

	/**
	 * Add the given {@link Consumer Consumers} to the list of delegates.
	 *
	 * @param consumers the {@link Consumer Consumers} to add
	 * @return {@literal this}
	 */
	public DelegatingConsumer<T> add(Collection<Consumer<T>> consumers) {
		if (null == consumers || consumers.isEmpty()) {
			return this;
		}
		for (Consumer<T> c : consumers) {
			add(c);
		}
		return this;
	}

	/**
	 * Remove the given {@link Consumer} from the list of delegates.
	 *
	 * @param consumer
	 * @return {@literal this}
	 */
	public DelegatingConsumer<T> remove(Consumer<T> consumer) {
		synchronized (delegateMonitor) {
			for (WeakReference<Consumer<T>> ref : delegates) {
				if (ref.get() == consumer) {
					ref.clear();
					break;
				}
			}
		}
		return this;
	}

	/**
	 * Remove the given {@link Consumer Consumers} from the list of delegates.
	 *
	 * @param consumers the {@link Consumer Consumers} to remove
	 * @return {@literal this}
	 */
	public DelegatingConsumer<T> remove(Collection<Consumer<T>> consumers) {
		if (null == consumers || consumers.isEmpty()) {
			return this;
		}
		for (Consumer<T> c : consumers) {
			remove(c);
		}
		prune();
		return this;
	}

	/**
	 * Remove the references to {@link Consumer Consumers} that have been removed or have gone out of scope and prune the
	 * size of the list. If many {@link Consumer Consumers} are removed individually, it will increase overall throughput
	 * to call this method periodically. To maintain as high a throughput as possible, though, its not necessary to call
	 * this every time a {@link Consumer} is removed.
	 *
	 * @return {@literal this}
	 */
	public DelegatingConsumer<T> prune() {
		synchronized (delegateMonitor) {
			List<WeakReference<Consumer<T>>> delegatesToRemove = new ArrayList<WeakReference<Consumer<T>>>();
			for (WeakReference<Consumer<T>> ref : delegates) {
				if (null == ref.get()) {
					delegatesToRemove.add(ref);
				}
			}
			delegates.removeAll(delegatesToRemove);
			delegateSize = delegates.size();
		}
		return this;
	}

	/**
	 * Clear all delegate {@link Consumer Consumers} from the list.
	 *
	 * @return {@literal this}
	 */
	public DelegatingConsumer<T> clear() {
		synchronized (delegateMonitor) {
			delegates.clear();
			delegateSize = delegates.size();
		}
		return this;
	}

	@Override
	public void accept(T t) {
		synchronized (delegateMonitor) {
			final int size = delegateSize;
			for (int i = 0; i < size; i++) {
				WeakReference<Consumer<T>> ref = delegates.get(i);
				if (null == ref) {
					continue;
				}
				Consumer<T> c = ref.get();
				if (null == c) {
					continue;
				}
				c.accept(t);
			}
		}
	}

	@Override
	@Nonnull
	public Iterator<Consumer<T>> iterator() {
		return new Iterator<Consumer<T>>() {
			final Iterator<WeakReference<Consumer<T>>> delegatesIter = delegates.iterator();

			public boolean hasNext() {
				return delegatesIter.hasNext();
			}

			@Override
			public Consumer<T> next() {
				return delegatesIter.next().get();
			}

			@Override
			public void remove() {
				delegatesIter.remove();
			}
		};
	}

  @Override
  public void start() {
    synchronized (delegateMonitor) {
      final int size = delegateSize;
      for (int i = 0; i < size; i++) {
        WeakReference<Consumer<T>> ref = delegates.get(i);
        if (null == ref) {
          continue;
        }
        Consumer<T> c = ref.get();
        if (null == c || !(c instanceof BatchConsumer)) {
          continue;
        }
        ((BatchConsumer) c).start();
      }
    }
  }

  @Override
  public void end() {
    synchronized (delegateMonitor) {
      final int size = delegateSize;
      for (int i = 0; i < size; i++) {
        WeakReference<Consumer<T>> ref = delegates.get(i);
        if (null == ref) {
          continue;
        }
        Consumer<T> c = ref.get();
        if (null == c || !(c instanceof BatchConsumer)) {
          continue;
        }
        ((BatchConsumer) c).end();
      }
    }
  }

}
