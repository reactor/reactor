package reactor.function;

import reactor.core.composable.Composable;
import reactor.core.composable.Deferred;
import reactor.core.composable.Stream;
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
  private final Deferred<A, Stream<A>> childDeferredStream;

  private final AtomicLong count;
  private final int batchSize;

  public ReduceConsumer(@Nonnull final Deferred<A, Stream<A>> childDeferredStream,
                        @Nonnull final Function<Tuple2<T, A>, A> fn,
                        @Nullable final Supplier<A> accumulators,
                        final int batchSize)
  {
    this.fn = fn;
    this.accumulators = accumulators;
    this.childDeferredStream = childDeferredStream;
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
      childDeferredStream.acceptEvent(value.copy(acc));
    } else if(!isBatch()) {
      childDeferredStream.acceptEvent(value.copy(acc));
    }
  }

  public Stream<A> getChildStream() {
    return childDeferredStream.compose();
  }


  public boolean isBatch() {
    return batchSize > 0;
  }
}
