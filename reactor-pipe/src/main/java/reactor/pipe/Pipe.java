package reactor.pipe;

import org.pcollections.PVector;
import org.pcollections.TreePVector;
import reactor.bus.Bus;
import reactor.fn.*;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;
import reactor.pipe.operation.PartitionOperation;
import reactor.pipe.operation.SlidingWindowOperation;
import reactor.pipe.state.DefaultStateProvider;
import reactor.pipe.state.StateProvider;
import reactor.pipe.stream.StreamSupplier;

import java.util.List;

@SuppressWarnings("unchecked")
public class Pipe<INIT, CURRENT> implements IPipe<Pipe, INIT, CURRENT> {

    private final StateProvider<Key> stateProvider;
    private final PVector<StreamSupplier> suppliers;

    protected Pipe() {
        this(TreePVector.empty(), new DefaultStateProvider<>());
    }

    protected Pipe(StateProvider<Key> stateProvider) {
        this(TreePVector.empty(), stateProvider);
    }

    protected Pipe(PVector<StreamSupplier> suppliers,
            StateProvider<Key> stateProvider) {
        this.suppliers = suppliers;
        this.stateProvider = stateProvider;
    }

    public <NEXT> Pipe<INIT, NEXT> map(Function<CURRENT, NEXT> mapper) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(Key src,
                    Key dst,
                    Bus<Key, Object> firehose) {
                return (key, value) -> {
                    firehose.notify(dst.clone(key), mapper.apply(value));
                };
            }
        });
    }

    public <NEXT> Pipe<INIT, NEXT> map(Supplier<Function<CURRENT, NEXT>> supplier) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(Key src,
                    Key dst,
                    Bus<Key, Object> firehose) {
                Function<CURRENT, NEXT> mapper = supplier.get();
                return (key, value) -> {
                    firehose.notify(dst.clone(key), mapper.apply(value));
                };
            }
        });
    }

    public <ST, NEXT> Pipe<INIT, NEXT> map(BiFunction<Atom<ST>, CURRENT, NEXT> mapper,
            ST init) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(Key src,
                    Key dst,
                    Bus<Key, Object> firehose) {
                Atom<ST> st = stateProvider.makeAtom(src, init);

                return (key, value) -> {
                    firehose.notify(dst.clone(key), mapper.apply(st, value));
                };
            }
        });
    }

    public <ST> Pipe<INIT, ST> scan(BiFunction<ST, CURRENT, ST> mapper,
            ST init) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(Key src,
                    Key dst,
                    Bus<Key, Object> firehose) {
                Atom<ST> st = stateProvider.makeAtom(src, init);

                return (key, value) -> {
                    ST newSt = st.update((old) -> mapper.apply(old, value));
                    firehose.notify(dst.clone(key), newSt);
                };
            }
        });
    }

    //  @Override
    //  public IPipe<INIT, CURRENT> debounce(long period, TimeUnit timeUnit) {
    //    return next(new StreamSupplier<Key, CURRENT>() {
    //      @Override
    //      public BiConsumer<Key, CURRENT> get(Key src, Key dst, Bus<Key, Object> firehose) {
    //        final Atom<CURRENT> debounced = stateProvider.makeAtom(src, null);
    //        final AtomicReference<Pausable> pausable = new AtomicReference<>(null);
    //
    //        return (key, value) -> {
    //          debounced.update(current -> value);
    //
    //          if (pausable.get() == null) {
    //            pausable.set(
    //              firehose.getTimer().submit(new Consumer<Long>() {
    //                @Override
    //                public void accept(Long v) {
    //                  firehose.notify(dst, debounced.deref());
    //                  pausable.set(null);
    //                }
    //              }, period, timeUnit));
    //          }
    //        };
    //      }
    //    });
    //  }

    //  @Override
    //  public IPipe<INIT, CURRENT> throttle(long period, TimeUnit timeUnit) {
    //    return next(new StreamSupplier<Key, CURRENT>() {
    //      @Override
    //      public BiConsumer<Key, CURRENT> get(Key src, Key dst, Bus<Key, Object> firehose) {
    //        final Atom<CURRENT> debounced = stateProvider.makeAtom(src, null);
    //        final AtomicReference<Pausable> pausable = new AtomicReference<>(null);
    //
    //        return (key, value) -> {
    //          Pausable oldScheduled = pausable.getAndUpdate((p) -> null);
    //          if (oldScheduled != null) {
    //            oldScheduled.cancel();
    //          }
    //
    //          debounced.update(current -> value);
    //
    //          pausable.set(firehose.getTimer().submit(new Consumer<Long>() {
    //            @Override
    //            public void accept(Long v) {
    //              firehose.notify(dst, debounced.deref());
    //              pausable.set(null);
    //            }
    //          }, period, timeUnit));
    //        };
    //      }
    //    });
    //  }

    public Pipe<INIT, CURRENT> filter(Predicate<CURRENT> predicate) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(Key src,
                    Key dst,
                    Bus<Key, Object> firehose) {
                return (key, value) -> {
                    if (predicate.test(value)) {
                        firehose.notify(dst.clone(key), value);
                    }
                };
            }
        });
    }

    public Pipe<INIT, List<CURRENT>> slide(UnaryOperator<List<CURRENT>> drop) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(Key src,
                    Key dst,
                    Bus<Key, Object> firehose) {
                Atom<PVector<CURRENT>> buffer = stateProvider.makeAtom(src, TreePVector.empty());

                return new SlidingWindowOperation<>(firehose,
                        buffer,
                        drop,
                        dst);
            }
        });
    }

    public Pipe<INIT, List<CURRENT>> partition(Predicate<List<CURRENT>> emit) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(Key src,
                    Key dst,
                    Bus<Key, Object> firehose) {
                Atom<PVector<CURRENT>> buffer = stateProvider.makeAtom(dst, TreePVector.empty());

                return new PartitionOperation<>(firehose,
                        buffer,
                        emit,
                        dst);
            }
        });
    }

    public Pipe<INIT, List<CURRENT>> custom(StreamSupplier<Key, CURRENT> supplier) {
        return next(supplier);
    }

    /**
     * STREAM ENDS
     */

    public <SRC extends Key> IPipeEnd consume(BiConsumer<SRC, CURRENT> consumer) {
        return end(new StreamSupplier<SRC, CURRENT>() {
            @Override
            public BiConsumer<SRC, CURRENT> get(SRC src,
                    Key dst,
                    Bus<Key, Object> firehose) {
                return consumer;
            }
        });
    }

    public IPipeEnd consume(Consumer<CURRENT> consumer) {
        return end(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(Key src,
                    Key dst,
                    Bus<Key, Object> pipe) {
                return (key, value) -> consumer.accept(value);
            }
        });
    }

    public <SRC extends Key> IPipeEnd consume(Supplier<BiConsumer<SRC, CURRENT>> supplier) {
        return end(new StreamSupplier<SRC, CURRENT>() {
            @Override
            public BiConsumer<SRC, CURRENT> get(SRC src,
                    Key dst,
                    Bus<Key, Object> pipe) {
                return supplier.get();
            }

        });
    }

    public static <A> Pipe<A, A> build() {
        return new Pipe<>();
    }

    public static <A> Pipe<A, A> build(StateProvider<Key> stateProvider) {
        return new Pipe<>(stateProvider);
    }

    protected <NEXT> Pipe<INIT, NEXT> next(StreamSupplier supplier) {
        return new Pipe<>(suppliers.plus(supplier),
                stateProvider);
    }

    protected <NEXT> IPipeEnd end(StreamSupplier supplier) {
        return new reactor.pipe.PipeEnd<>(suppliers.plus(supplier));
    }

}
