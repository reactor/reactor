package reactor.core.alloc;

import reactor.core.alloc.factory.EventFactorySupplier;
import reactor.event.Event;

import java.util.HashMap;

/**
 * Generic Event Allocator. Allocates events based on their generic type.
 *
 * @author Oleksandr Petrov
 */
public abstract class EventAllocator {

  private final Object                     monitor;
  private final HashMap<Class, Allocator>  eventPools;

  public EventAllocator() {
    this(new Class[0]);
  }

  /**
   * Create a new {@link reactor.core.alloc.EventAllocator}, containing pre-created
   * {@link reactor.core.alloc.Allocator}s for given {@data class}es.
   *
   * @param classes
   */
  public EventAllocator(Class[] classes) {
    this.eventPools = new HashMap<Class, Allocator>();
    this.monitor = new Object();
    for(Class c: classes) {
      eventPools.put(c, makeAllocator(c));
    }
  }

  /**
   * Allocate an object from the internal pool, based on the type of Event.
   *
   * @param klass generic type of {@link reactor.event.Event}
   * @param <T> generic type of {@link reactor.event.Event}
   *
   * @return a {@link reactor.core.alloc.Reference} that can be retained and released.
   */
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
   * Make a new allocator for {@link reactor.event.Event}s with generic type of {@data klass}
   *
   * @param klass generic type of {@link reactor.event.Event}
   * @param <T> generic type of {@link reactor.event.Event}
   */
  protected abstract <T> Allocator<Event<T>> makeAllocator(Class<T> klass);

  /**
   * Default Event Allocator, uses {@link reactor.core.alloc.ReferenceCountingAllocator} for
   * allocating and recycling events.
   *
   * @return newly constructed event alloator.
   */
  public static EventAllocator defaultEventAllocator() {
    return new EventAllocator() {
      @Override
      protected <T> Allocator<Event<T>> makeAllocator(Class<T> klass) {
        return new ReferenceCountingAllocator<Event<T>>(new EventFactorySupplier(klass));
      }
    };
  }
}
