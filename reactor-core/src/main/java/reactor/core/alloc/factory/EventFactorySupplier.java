package reactor.core.alloc.factory;

import reactor.event.Event;
import reactor.function.Supplier;

/**
 * A {@link reactor.function.Supplier} implementation that instantiates Events
 * based on Event data type.
 *
 * @param <T> type of {@link reactor.event.Event} data
 * @author Oleksandr Petrov
 * @since 1.1
 */
public class EventFactorySupplier<T> implements Supplier<Event<T>> {

  private final Class<T> klass;

  public EventFactorySupplier(Class<T> klass) {
    this.klass = klass;
  }

  @Override
  public Event<T> get() {
    return new Event<T>(klass);
  }
}
