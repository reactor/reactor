package reactor.function;

import reactor.core.composable.Composable;
import reactor.core.composable.Deferred;
import reactor.event.Event;
import javax.annotation.Nonnull;

public class MapConsumer<T, R> implements Consumer<Event<T>> {

  private final Function<T, R> fn;
  private final Deferred<R, ? extends Composable<R>> childDeferredStream;

  public MapConsumer(@Nonnull Deferred<R, ? extends Composable<R>> childDeferredStream,
                     @Nonnull Function<T, R> fn) {
    this.fn = fn;
    this.childDeferredStream = childDeferredStream;
  }

  @Override
  public void accept(Event<T> value) {
    try {
      R val = fn.apply(value.getData());
      childDeferredStream.acceptEvent(value.copy(val));
    } catch (Throwable e) {
      childDeferredStream.accept(e);
    }
  }

  public Composable<R> getChildStream() {
    return childDeferredStream.compose();
  }
}
