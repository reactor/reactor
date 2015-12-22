package reactor.pipe;

import reactor.bus.AbstractBus;
import reactor.bus.Bus;
import reactor.bus.selector.Selector;
import reactor.fn.*;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Pipe represents a streaming transformation from `INIT` type,
 * which is an initial type of the topolgy, to the `CURRENT` type.
 */
public interface IPipe<COVARIANT extends IPipe, INIT, CURRENT> {

    /**
     * Takes every incoming item of the `CURRENT` type and transforms
     * it to `NEXT` type.
     *
     * <a href='http://rxmarbles.com/#map'>RxMarbles example on map</a>
     * @param mapper mapper function, that will be called for each incoming item
     * @param <NEXT> type of the next item
     * @return pipe of the {@code NEXT} type
     */
    <NEXT> IPipe<COVARIANT, INIT, NEXT> map(Function<CURRENT, NEXT> mapper);

    /**
     * Same as {@code map}, possibly with side-effects (for example, state saved
     * over the calls).
     *
     * Takes every incoming item of the `CURRENT` type and transforms
     * it to `NEXT` type.
     *
     * <a href='http://rxmarbles.com/#map'>RxMarbles example on map</a>
     * @param supplier function supplier, that yields a function instance will be called
     *                 for each subscription.
     * @param <NEXT> type of the next item
     * @return pipe of the {@code NEXT} type
     */
    <NEXT> IPipe<COVARIANT, INIT, NEXT> map(Supplier<Function<CURRENT, NEXT>> supplier);

    /**
     * Same as {@code map}, that allows managing the intermediate state in the {@code Atom}.
     * {@code Atom} is passed to the {@code mapper} as a first argument.
     *
     * Takes every incoming item of the `CURRENT` type and transforms
     * it to `NEXT` type.
     *
     * <a href='http://rxmarbles.com/#map'>RxMarbles example on map</a>
     * @param mapper mapper function, that will be called for each incoming item
     * @param init initial state of the {@code Atom}
     * @param <ST> type of the saved intermediate State
     * @param <NEXT> type of the next item
     * @return pipe of the {@code NEXT} type
     */
    <ST, NEXT> IPipe<COVARIANT, INIT, NEXT> map(BiFunction<Atom<ST>, CURRENT, NEXT> mapper,
                                     ST init);

    /**
     * Reduces the stream to some accumulator value. Accumulator value is saved
     * between the calls.
     *
     * <a href='http://rxmarbles.com/#scan'>RxMarbles example on scan</a>
     * @param mapper function that receives an intermediate state and every incoming item,
     *               yielding resulting state, that will be saved until the next call.
     * @param init inital state
     * @param <ST> type of the saved intermediate state
     * @return pipe of the {@code NEXT} type
     */
    <ST> IPipe<COVARIANT, INIT, ST> scan(BiFunction<ST, CURRENT, ST> mapper,
                              ST init);

    /**
     * Debounces the stream, which postpones calling the next step within current pipe
     * until after wait time specified by {@code period} and {@code timeUnit} elapsed since
     * the first time it was called.
     *
     * @param period time period
     * @param timeUnit time period unit
     * @return pipe of the {@code CURRENT} type
     */
    IPipe<COVARIANT, INIT, CURRENT> debounce(long period, TimeUnit timeUnit);

    /**
     * Throttles the stream, which postpones calling the next step within current pipe
     * until after wait time specified by {@code period} and {@code timeUnit} elapsed since
     * the last time it was called.
     *
     * @param period time period
     * @param timeUnit time period unit
     * @return pipe of the {@code CURRENT} type
     */
    IPipe<COVARIANT, INIT, CURRENT> throttle(long period, TimeUnit timeUnit);

    /**
     * Filters the stream. Next step within the current pipe will be called only for the
     * items for which {@code predicate} returns {@code true}.
     *
     * @param predicate that will be called for each item
     * @return pipe of the {@code CURRENT} type
     */
    IPipe<COVARIANT, INIT, CURRENT> filter(Predicate<CURRENT> predicate);

    /**
     * Creates a window that collects all incoming items into the list, allowing
     * dropping items based on the certain logic. For example, you could store only
     * last (@code 5} items within the stream.
     *
     * List resulting after {@code drop} function will be passed to the next step within
     * the current pipe.
     *
     * @param drop dropping logic function
     * @return pipe of the {@code List<CURRENT>} type
     */
    IPipe<COVARIANT, INIT, List<CURRENT>> slide(UnaryOperator<List<CURRENT>> drop);

    /**
     * Partitions the stream into the lists. For example you could collect a window
     * of {@code 5} items, and collect/process them on the next step.
     *
     * @param emit predicates that indicates that window is full.
     * @return pipe of the collected {@code<CURRENT>} items
     */
    IPipe<COVARIANT, INIT, List<CURRENT>> partition(Predicate<List<CURRENT>> emit);

    /**
     * Consumes the results of the current (@code IPipe}
     *
     * @param consumer that receives a dispatch {@code Key} and each value coming
     *                 into this stream.
     * @param <SRC> type of the key
     * @return {@code IPipeEnd}, the end of the current pipe
     */
    <SRC extends Key> IPipeEnd<INIT, CURRENT> consume(BiConsumer<SRC, CURRENT> consumer);

    /**
     * Consumes the results of the current (@code IPipe}, possibly inducing some side-effects.
     *
     * @param supplier that receives a dispatch {@code Key} and each value coming
     *                 into this stream.
     * @param <SRC> type of the key
     * @return {@code IPipeEnd}, the end of the current pipe
     */
    <SRC extends Key> IPipeEnd<INIT, CURRENT> consume(Supplier<BiConsumer<SRC, CURRENT>> supplier);

    /**
     * Consumes the results of the current (@code IPipe} without a {@code Key}
     *
     * @param consumer that receives each value coming into this stream.
     * @return {@code IPipeEnd}, the end of the current pipe
     */
    IPipeEnd<INIT, CURRENT> consume(Consumer<CURRENT> consumer);

    interface IPipeEnd<INIT, CURRENT> {
        /**
         * Subscribe the current Pipe to the {@code AbstractBus}.
         *
         * @param key subscription key
         * @param bus bus this pipe should dispatch on
         */
        void subscribe(Key key, AbstractBus<Key, Object> bus);

        /**
         * Subscribe the current Pipe to the {@code AbstractBus} based on some complex {@code Selector}
         *
         * @param matcher selector function, that will be called every time there's no direct
         *                {@code Key} match found.
         * @param bus bus the pipe should dispatch on
         */
        void subscribe(Selector<Key> matcher, AbstractBus<Key, Object> bus);

        /**
         * Subscribe the current {@code IPipe} to the {@code AbstractBus} based on the {@code Predicate}
         * @param matcher predicate function, that will be called every time there's no direct {@code Key}
         *                match found
         * @param bus bus the pipe should dispatch on
         */
        void subscribe(Predicate<Key> matcher, AbstractBus<Key, Object> bus);
    }
}
