package reactor.function;

import reactor.core.Observable;
import reactor.event.Event;
import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicLong;

public class ReduceConsumer<T, A> implements Consumer<Event<T>> {

  private final Function<Tuple2<T, A>, A> fn;
  private final Supplier<A> accumulators;
  private A acc;
  private final Observable observable;
  private final Object key;

  private final AtomicLong count;
  private final int batchSize;

  public ReduceConsumer(@Nonnull Observable observable,
                        @Nonnull Object key,
                        @Nonnull final Function<Tuple2<T, A>, A> fn,
                        @Nullable final Supplier<A> accumulators,
                        final int batchSize)
  {
    this.fn = fn;
    this.accumulators = accumulators;
    this.observable = observable;
    this.key = key;
    this.count = new AtomicLong(0);
    this.batchSize = batchSize;
  }

  @Override
  public void accept(Event<T> value) {
    if(null == acc) {
      acc = (null != accumulators ? accumulators.get() : null);
    }
    acc = fn.apply(Tuple.of(value.getData(), acc));

    if(isBatch() && count.incrementAndGet() % batchSize == 0) {
      observable.notify(key, value.copy(acc));
    } else if(!isBatch()) {
      observable.notify(key, value.copy(acc));
    }
  }

  public boolean isBatch() {
    return batchSize > 0;
  }
}
