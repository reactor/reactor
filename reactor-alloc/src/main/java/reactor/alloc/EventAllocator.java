/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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

package reactor.alloc;

import reactor.bus.Event;

import java.util.HashMap;

/**
 * Generic Event Allocator. Allocates events based on their generic type.
 *
 * @author Oleksandr Petrov
 */
public abstract class EventAllocator {

	private final Object                    monitor;
	private final HashMap<Class, Allocator> eventPools;

	public EventAllocator() {
		this(new Class[0]);
	}

	/**
	 * Create a new {@link EventAllocator}, containing pre-created
	 * {@link reactor.alloc.Allocator}s for given {@data class}es.
	 *
	 * @param classes
	 */
	@SuppressWarnings("unchecked")
	public EventAllocator(Class[] classes) {
		this.eventPools = new HashMap<Class, Allocator>();
		this.monitor = new Object();
		for (Class c : classes) {
			eventPools.put(c, makeAllocator(c));
    }
  }

  /**
   * Allocate an object from the internal pool, based on the type of Event.
   *
   * @param klass generic type of {@link reactor.bus.Event}
   * @param <T> generic type of {@link reactor.bus.Event}
   *
   * @return a {@link reactor.alloc.Reference} that can be retained and released.
   */
  @SuppressWarnings("unchecked")
  public <T> Reference<Event<T>> get(Class<T> klass) {
    if(!eventPools.containsKey(klass)) {
      synchronized (monitor) {
        // Check once again if another thread didn't create a supplier in a meanwhile
        if(!eventPools.containsKey(klass)) {
          eventPools.put(klass, makeAllocator(klass));
        }
      }
    }
    return eventPools.get(klass).allocate();
  }

  /**
   * Make a new allocator for {@link reactor.bus.Event}s with generic type of {@data klass}
   *
   * @param klass generic type of {@link reactor.bus.Event}
   * @param <T> generic type of {@link reactor.bus.Event}
   */
  protected abstract <T> Allocator<Event<T>> makeAllocator(Class<T> klass);

  /**
   * Default Event Allocator, uses {@link reactor.alloc.ReferenceCountingAllocator} for
   * allocating and recycling events.
   *
   * @return newly constructed event alloator.
   */
  public static EventAllocator defaultEventAllocator() {
    return new EventAllocator() {
      @SuppressWarnings("unchecked")
      @Override
      protected <T> Allocator<Event<T>> makeAllocator(Class<T> klass) {
        return new ReferenceCountingAllocator<Event<T>>(new EventFactorySupplier(klass));
      }
    };
  }
}
