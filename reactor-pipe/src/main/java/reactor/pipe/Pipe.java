package reactor.pipe;

import org.pcollections.PVector;
import org.pcollections.TreePVector;
import reactor.Timers;
import reactor.bus.Bus;
import reactor.core.support.ReactiveState.Pausable;
import reactor.core.timer.Timer;
import reactor.fn.*;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.concurrent.LazyVar;
import reactor.pipe.key.Key;
import reactor.pipe.operation.PartitionOperation;
import reactor.pipe.operation.SlidingWindowOperation;
import reactor.pipe.state.DefaultStateProvider;
import reactor.pipe.state.StateProvider;
import reactor.pipe.stream.StreamSupplier;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("unchecked")
public class Pipe<INIT, CURRENT> implements IPipe<Pipe, INIT, CURRENT> {

    private final StateProvider<Key>      stateProvider;
    private final PVector<StreamSupplier> suppliers;
    private final LazyVar<Timer>          timer;

    protected Pipe() {
        this(TreePVector.<StreamSupplier>empty(), new DefaultStateProvider<Key>());
    }

    protected Pipe(StateProvider<Key> stateProvider) {
        this(TreePVector.<StreamSupplier>empty(), stateProvider);
    }

    protected Pipe(TreePVector<StreamSupplier> suppliers,
                   StateProvider<Key> stateProvider) {
        this.suppliers = suppliers;
        this.stateProvider = stateProvider;
        this.timer = new LazyVar<>(new Supplier<Timer>() {
            @Override
            public Timer get() {
                return Timers.create("pipe-timer", 10, 512);
            }
        });
    }

    public <NEXT> Pipe<INIT, NEXT> map(final Function<CURRENT, NEXT> mapper) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(final Key src,
                                                final Key dst,
                                                final Bus<Key, Object> firehose) {
                return new BiConsumer<Key, CURRENT>() {
                    @Override
                    public void accept(Key key, CURRENT value) {
                        firehose.notify(dst.clone(key), mapper.apply(value));
                    }
                };
            }
        });
    }

    public <NEXT> Pipe<INIT, NEXT> map(final Supplier<Function<CURRENT, NEXT>> supplier) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(Key src,
                                                final Key dst,
                                                final Bus<Key, Object> firehose) {
                final Function<CURRENT, NEXT> mapper = supplier.get();
                return new BiConsumer<Key, CURRENT>() {
                    @Override
                    public void accept(Key key, CURRENT value) {
                        firehose.notify(dst.clone(key), mapper.apply(value));
                    }
                };
            }
        });
    }

    public <ST, NEXT> Pipe<INIT, NEXT> map(final BiFunction<Atom<ST>, CURRENT, NEXT> mapper,
                                           final ST init) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(final Key src,
                                                final Key dst,
                                                final Bus<Key, Object> firehose) {
                final Atom<ST> st = stateProvider.makeAtom(src, init);

                return new BiConsumer<Key, CURRENT>() {
                    @Override
                    public void accept(Key key, CURRENT value) {
                        firehose.notify(dst.clone(key), mapper.apply(st, value));
                    }
                };
            }
        });
    }

    public <ST> Pipe<INIT, ST> scan(final BiFunction<ST, CURRENT, ST> mapper,
                                    final ST init) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(Key src,
                                                final Key dst,
                                                final Bus<Key, Object> firehose) {
                final Atom<ST> st = stateProvider.makeAtom(src, init);

                return new BiConsumer<Key, CURRENT>() {
                    @Override
                    public void accept(final Key key,
                                       final CURRENT value) {
                        ST newSt = st.update(new UnaryOperator<ST>() {
                            @Override
                            public ST apply(ST old) {
                                return mapper.apply(old, value);
                            }
                        });
                        firehose.notify(dst.clone(key), newSt);
                    }
                };
            }
        });
    }

    @Override
    public Pipe<INIT, CURRENT> throttle(final long period,
                                        final TimeUnit timeUnit) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(Key src, final Key dst, final Bus<Key, Object> firehose) {
                final Atom<CURRENT> debounced = stateProvider.makeAtom(src, null);
                final AtomicReference<Pausable> pausable = new AtomicReference<>(null);

                Consumer<Long> notifyConsumer = new Consumer<Long>() {
                    @Override
                    public void accept(Long v) {
                        firehose.notify(dst, debounced.deref());
                    }
                };

                return new BiConsumer<Key, CURRENT>() {
                    @Override
                    public void accept(final Key key,
                                       final CURRENT value) {
                        debounced.reset(value);

                        if (pausable.get() == null) {
                            pausable.set(timer.get().submit(notifyConsumer, period, timeUnit));
                        }
                    }
                };
            }
        });
    }

    @Override
    public Pipe<INIT, CURRENT> debounce(final long period,
                                        final TimeUnit timeUnit) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(Key src, final Key dst, final Bus<Key, Object> firehose) {
                final Atom<CURRENT> debouncedValue = stateProvider.makeAtom(src, null);
                final AtomicReference<Pausable> pausable = new AtomicReference<>(null);

                final Consumer<Long> notifyConsumer = new Consumer<Long>() {
                    @Override
                    public void accept(Long v) {
                        firehose.notify(dst, debouncedValue.deref());
                    }
                };

                return new BiConsumer<Key, CURRENT>() {
                    @Override
                    public void accept(final Key key,
                                       final CURRENT value) {
                        Pausable oldScheduled = pausable.getAndSet(null);

                        if (oldScheduled != null) {
                            oldScheduled.cancel();
                        }

                        debouncedValue.reset(value);

                        pausable.set(timer.get().submit(notifyConsumer, period, timeUnit));
                    }
                };
            }
        });
    }

    public Pipe<INIT, CURRENT> filter(final Predicate<CURRENT> predicate) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(final Key src,
                                                final Key dst,
                                                final Bus<Key, Object> firehose) {
                return new BiConsumer<Key, CURRENT>() {
                    @Override
                    public void accept(Key key, CURRENT value) {
                        if (predicate.test(value)) {
                            firehose.notify(dst.clone(key), value);
                        }
                    }
                };
            }
        });
    }

    public Pipe<INIT, List<CURRENT>> slide(final UnaryOperator<List<CURRENT>> drop) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(Key src,
                                                Key dst,
                                                Bus<Key, Object> firehose) {
                Atom<PVector<CURRENT>> buffer = stateProvider.makeAtom(src,
                                                                       (PVector<CURRENT>)
                                                                           TreePVector.<CURRENT>empty());

                return new SlidingWindowOperation<>(firehose,
                                                    buffer,
                                                    drop,
                                                    dst);
            }
        });
    }

    public Pipe<INIT, List<CURRENT>> partition(final Predicate<List<CURRENT>> emit) {
        return next(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(final Key src,
                                                final Key dst,
                                                final Bus<Key, Object> firehose) {
                Atom<PVector<CURRENT>> buffer = stateProvider.makeAtom(dst,
                                                                       (PVector<CURRENT>)
                                                                           TreePVector.<CURRENT>empty());

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

    public <SRC extends Key> IPipeEnd consume(final BiConsumer<SRC, CURRENT> consumer) {
        return end(new StreamSupplier<SRC, CURRENT>() {
            @Override
            public BiConsumer<SRC, CURRENT> get(SRC src,
                                                Key dst,
                                                Bus<Key, Object> firehose) {
                return consumer;
            }
        });
    }

    public IPipeEnd consume(final Consumer<CURRENT> consumer) {
        return end(new StreamSupplier<Key, CURRENT>() {
            @Override
            public BiConsumer<Key, CURRENT> get(Key src,
                                                Key dst,
                                                Bus<Key, Object> pipe) {
                return new BiConsumer<Key, CURRENT>() {
                    @Override
                    public void accept(Key key, CURRENT value) {
                        consumer.accept(value);
                    }
                };
            }
        });
    }

    public <SRC extends Key> IPipeEnd consume(final Supplier<BiConsumer<SRC, CURRENT>> supplier) {
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
        return new Pipe<>((TreePVector<StreamSupplier>) suppliers.plus(supplier),
                          stateProvider);
    }

    protected <NEXT> IPipeEnd end(StreamSupplier supplier) {
        return new reactor.pipe.PipeEnd<>(suppliers.plus(supplier));
    }

}
