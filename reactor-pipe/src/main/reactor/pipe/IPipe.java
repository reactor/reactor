package reactor.pipe;

import reactor.fn.BiConsumer;
import reactor.fn.BiFunction;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.fn.Supplier;
import reactor.fn.UnaryOperator;
import reactor.pipe.concurrent.Atom;
import reactor.bus.Bus;
import reactor.bus.selector.Selector;
import reactor.pipe.key.Key;

import java.util.List;

/**
 * Pipe represents a streaming transformation from `INIT` type,
 * which is an initial type of the topolgy, to the `CURRENT` type.
 */
public interface IPipe<INIT, CURRENT> {

  <NEXT> IPipe<INIT, NEXT> map(Function<CURRENT, NEXT> mapper);

  <NEXT> IPipe<INIT, NEXT> map(Supplier<Function<CURRENT, NEXT>> supplier);

  <ST, NEXT> IPipe<INIT, NEXT> map(BiFunction<Atom<ST>, CURRENT, NEXT> mapper,
                                   ST init);

  <ST> IPipe<INIT, ST> scan(BiFunction<ST, CURRENT, ST> mapper,
                            ST init);

//  IPipe<INIT, CURRENT> debounce(long period, TimeUnit timeUnit);
//
//  IPipe<INIT, CURRENT> throttle(long period, TimeUnit timeUnit);

  IPipe<INIT, CURRENT> filter(Predicate<CURRENT> predicate);

  IPipe<INIT, List<CURRENT>> slide(UnaryOperator<List<CURRENT>> drop);

  IPipe<INIT, List<CURRENT>> partition(Predicate<List<CURRENT>> emit);

  <SRC extends Key> IPipeEnd<INIT, CURRENT> consume(BiConsumer<SRC, CURRENT> consumer);

  <SRC extends Key> IPipeEnd<INIT, CURRENT> consume(Supplier<BiConsumer<SRC, CURRENT>> supplier);

  IPipeEnd<INIT, CURRENT> consume(Consumer<CURRENT> consumer);


  interface IPipeEnd<INIT, CURRENT> {
    void subscribe(Key key, Bus<Key, Object> firehose);
    void subscribe(Selector<Key> matcher, Bus<Key, Object> firehose);
    void subscribe(Predicate<Key> matcher, Bus<Key, Object> firehose);
  }
}
