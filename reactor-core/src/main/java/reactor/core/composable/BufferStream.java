package reactor.core.composable;

import reactor.core.action.*;
import reactor.event.Event;
import reactor.core.Environment;
import reactor.core.Observable;
import reactor.event.selector.Selector;
import reactor.function.Function;
import reactor.function.Supplier;
import reactor.tuple.Tuple2;
import reactor.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class BufferStream<T> extends Stream<T> {

  final int             batchSize;
  final BufferAction<T> bufferAction;
  final Iterable<T>     values;

  public BufferStream(@Nullable Observable observable,
                      int batchSize,
                      @Nullable Iterable<T> values,
                      @Nullable final Composable<?> parent,
                      @Nullable Tuple2<Selector, Object> acceptSelector,
                      @Nullable Environment environment) {
    super(observable, parent, acceptSelector, environment);
    this.batchSize = batchSize;
    this.bufferAction = new BufferAction<T>(batchSize, getObservable(), getAcceptKey(), getError().getObject());
    this.values = values;
  }

  @Override
  public String debug() {
    Composable<?> that = this;
    while (that.getParent() != null) {
      that = that.getParent();
    }
    if(that == this){
      return ActionUtils.browseAction(bufferAction);
    }else{
      return that.debug();
    }
  }

  @Override
  void notifyValue(Event<T> value) {
    bufferAction.accept(value);
  }

  @Override
  void notifyFlush() {
    if (values != null) {
      for (T val : values) {
        bufferAction.accept(Event.wrap(val));
      }
    }
  }

  /**
   * Create a new {@code Stream} whose values will be only the first value of each batch. Requires a {@code batchSize}
   * to
   * have been set.
   * <p/>
   * When a new batch is triggered, the first value of that next batch will be pushed into this {@code Stream}.
   *
   * @return a new {@code Stream} whose values are the first value of each batch
   */
  public BufferStream<T> first() {
    return first(batchSize);
  }

  /**
   * Create a new {@code Stream} whose values will be only the first value of each batch. Requires a {@code batchSize}
   * to
   * have been set.
   * <p/>
   * When a new batch is triggered, the first value of that next batch will be pushed into this {@code Stream}.
   *
   * @param batchSize the batch size to use
   * @return a new {@code Stream} whose values are the first value of each batch)
   */
  public BufferStream<T> first(int batchSize) {
    Assert.state(batchSize > 0, "Cannot first() an unbounded Stream. Try extracting a batch first.");
    final Deferred<T, BufferStream<T>> d = createDeferredChildStream(batchSize);
    add(new BatchAction<T>(batchSize,
                           getObservable(),
                           null,
                           getError().getObject(),
                           null,
                           d.compose().getAcceptKey()));
    return d.compose();
  }

  private Deferred<T, BufferStream<T>> createDeferredChildStream(int batchSize) {
    BufferStream<T> stream = new BufferStream<T>(null,
                                                 batchSize,
                                                 null,
                                                 this,
                                                 null,
                                                 environment);

    return new Deferred<T, BufferStream<T>>(stream);
  }


  /**
   * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code batchSize}
   *
   * @return a new {@code Stream} whose values are the last value of each batch
   */
  public BufferStream<T> last() {
    return last(batchSize);
  }

  /**
   * Create a new {@code Stream} whose values will be only the last value of each batch. Requires a {@code batchSize}
   *
   * @param batchSize the batch size to use
   * @return a new {@code Stream} whose values are the last value of each batch
   */
  public BufferStream<T> last(int batchSize) {
    Assert.state(batchSize > 0, "Cannot last() an unbounded Stream. Try extracting a batch first.");
    final Deferred<T, BufferStream<T>> d = createDeferredChildStream(batchSize);
    add(new BatchAction<T>(batchSize,
                           getObservable(),
                           null,
                           getError().getObject(),
                           d.compose().getAcceptKey(),
                           null));
    return d.compose();
  }


  /**
   * Create a new {@code Stream} whose values will be each element E of any Iterable<E> flowing this Stream
   * <p/>
   * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
   *
   * @return a new {@code Stream} whose values result from the iterable input
   */
  public Stream<T> split() {
    return split(batchSize);
  }

  /**
   * Create a new {@code Stream} whose values will be each element E of any Iterable<E> flowing this Stream
   * <p/>
   * When a new batch is triggered, the last value of that next batch will be pushed into this {@code Stream}.
   *
   * @param batchSize the batch size to use
   * @return a new {@code Stream} whose values result from the iterable input
   */
  public Stream<T> split(int batchSize) {
    final Deferred<T, Stream<T>> d = createDeferred(batchSize);
    getObservable().on(getAcceptSelector(),
                       new ForEachAction<T>(batchSize, getObservable(), d.compose().getAcceptKey(), getError().getObject()));
    return d.compose();
  }

  private Deferred<Iterable<T>, BufferStream<Iterable<T>>> createDeferredIterableChildStream(int batchSize) {
    BufferStream<Iterable<T>> stream = new BufferStream<Iterable<T>>(null,
                                                               batchSize,
                                                               null,
                                                               this,
                                                               null,
                                                               environment);
    return new Deferred<Iterable<T>, BufferStream<Iterable<T>>>(stream);
  }

  /**
   * Indicates whether or not this {@code Stream} is unbounded.
   *
   * @return {@literal true} if a {@code batchSize} has been set, {@literal false} otherwise
   */
  @Override
  public boolean isBatch() {
    return true;
  }

  /**
   * Reduce the values passing through this {@code Stream} into an object {@code A}. The given {@link reactor.function.Supplier} will be
   * used to produce initial accumulator objects either on the first reduce call, in the case of an unbounded {@code
   * Stream}, or on the first value of each batch, if a {@code batchSize} is set.
   * <p/>
   * In an unbounded {@code Stream}, the accumulated value will be published on the returned {@code Stream} every time
   * a
   * value is accepted. But when a {@code batchSize} has been set and {@link #isBatch()} returns true, the accumulated
   * value will only be published on the new {@code Stream} at the end of each batch. On the next value (the first of
   * the next batch), the {@link reactor.function.Supplier} is called again for a new accumulator object and the reduce starts over with
   * a new accumulator.
   *
   * @param fn           the reduce function
   * @param accumulators the {@link reactor.function.Supplier} that will provide accumulators
   * @param <A>          the type of the reduced object
   * @return a new {@code Stream} whose values contain only the reduced objects
   */
  public <A> Stream<A> reduce(@Nonnull final Function<Tuple2<T, A>, A> fn, @Nullable final Supplier<A> accumulators) {
    final Deferred<A, Stream<A>> d = createDeferred(batchSize);
    final Stream<A> stream = d.compose();

    add(new ReduceAction<T, A>(batchSize,
                               accumulators,
                               fn,
                               stream.getObservable(), stream.getAcceptKey(), getError().getObject()
      ));
    return d.compose();
  }

  /**
   * Collect incoming values into a {@link List} that will be pushed into the returned {@code Stream} every time {@code
   * batchSize} has been reached.
   *
   * @return a new {@code Stream} whose values are a {@link List} of all values in this batch
   */
  public Stream<Iterable<T>> collect() {
    Assert.state(batchSize > 0, "Cannot collect() an unbounded Stream. Try extracting a batch first.");
    return collect(batchSize);
  }

  @SuppressWarnings("unchecked")
  protected <V, C extends Composable<V>> Deferred<V, C> createDeferred() {
    return createDeferred(batchSize);
  }
}