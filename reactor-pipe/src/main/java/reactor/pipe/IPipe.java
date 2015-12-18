package reactor.pipe;

import reactor.bus.Bus;
import reactor.bus.selector.Selector;
import reactor.fn.*;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;

import java.util.List;

/**
 * Pipe represents a streaming transformation from `INIT` type,
 * which is an initial type of the topolgy, to the `CURRENT` type.
 */
public interface IPipe<COVARIANT extends IPipe, INIT, CURRENT> {

    <NEXT> IPipe<COVARIANT, INIT, NEXT> map(Function<CURRENT, NEXT> mapper);

    <NEXT> IPipe<COVARIANT, INIT, NEXT> map(Supplier<Function<CURRENT, NEXT>> supplier);

    <ST, NEXT> IPipe<COVARIANT, INIT, NEXT> map(BiFunction<Atom<ST>, CURRENT, NEXT> mapper,
                                     ST init);

    <ST> IPipe<COVARIANT, INIT, ST> scan(BiFunction<ST, CURRENT, ST> mapper,
                              ST init);

    //  IPipe<INIT, CURRENT> debounce(long period, TimeUnit timeUnit);
    //
    //  IPipe<INIT, CURRENT> throttle(long period, TimeUnit timeUnit);

    IPipe<COVARIANT, INIT, CURRENT> filter(Predicate<CURRENT> predicate);

    IPipe<COVARIANT, INIT, List<CURRENT>> slide(UnaryOperator<List<CURRENT>> drop);

    IPipe<COVARIANT, INIT, List<CURRENT>> partition(Predicate<List<CURRENT>> emit);

    <SRC extends Key> IPipeEnd<INIT, CURRENT> consume(BiConsumer<SRC, CURRENT> consumer);

    <SRC extends Key> IPipeEnd<INIT, CURRENT> consume(Supplier<BiConsumer<SRC, CURRENT>> supplier);

    IPipeEnd<INIT, CURRENT> consume(Consumer<CURRENT> consumer);

    interface IPipeEnd<INIT, CURRENT> {
        void subscribe(Key key, Bus<Key, Object> firehose);

        void subscribe(Selector<Key> matcher, Bus<Key, Object> firehose);

        void subscribe(Predicate<Key> matcher, Bus<Key, Object> firehose);
    }
}
