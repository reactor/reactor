package reactor.function;

import reactor.core.Observable;
import reactor.event.Event;
import javax.annotation.Nonnull;

public class MapConsumer<T, R> implements Consumer<Event<T>> {

  private final Function<T, R> fn;
  private final Observable observable;
  private final Object key;

  public MapConsumer(@Nonnull Observable observable,
                     @Nonnull Object key,
                     @Nonnull Function<T, R> fn) {
    this.fn = fn;
    this.observable = observable;
    this.key = key;
  }

  @Override
  public void accept(Event<T> value) {
    try {
      R val = fn.apply(value.getData());

      observable.notify(key, value.copy(val));
    } catch (Throwable e) {
      observable.notify(e.getClass(), Event.wrap(e));
    }
  }
}
