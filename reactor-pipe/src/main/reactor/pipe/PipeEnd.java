package reactor.pipe;

import org.pcollections.PVector;
import reactor.bus.Bus;
import reactor.bus.selector.PredicateSelector;
import reactor.bus.selector.Selector;
import reactor.fn.BiConsumer;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.pipe.key.Key;
import reactor.pipe.stream.StreamSupplier;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * FinalizedMatchedPipe represents a stream builder that can take values
 * of `INIT` and transform them via the pipeline to `FINAL` type.
 */
public class PipeEnd<INIT, FINAL> implements IPipe.IPipeEnd<INIT, FINAL> {

    private final PVector<StreamSupplier> suppliers;

    protected PipeEnd(PVector<StreamSupplier> suppliers) {
        this.suppliers = suppliers;
    }


    @Override
    public void subscribe(Key key, Bus<Key, Object> firehose) {
        Key currentKey = key;
        for (StreamSupplier supplier : suppliers) {
            Key nextKey = currentKey.derive();
            firehose.onKey(currentKey, supplier.get(currentKey, nextKey, firehose));
            currentKey = nextKey;
        }
    }

    @Override
    public void subscribe(Selector<Key> matcher, Bus<Key, Object> firehose) {
        firehose.on(matcher,
                    new BiConsumer<Key, Object>() {
                        @Override
                        public void accept(Key key, Object o) {
                            subscribers(firehose).apply(key).forEach((k, consumer) -> {
                                firehose.onKey(k, consumer);
                            });
                        }
                    });
    }

    @Override
    public void subscribe(Predicate<Key> matcher, Bus<Key, Object> firehose) {
        firehose.on(new PredicateSelector<Key>(matcher),
                    new BiConsumer<Key, Object>() {
                        @Override
                        public void accept(Key key, Object o) {
                            subscribers(firehose).apply(key).forEach((k, consumer) -> {
                                firehose.onKey(k, consumer);
                            });
                        }
                    });
    }

    private Function<Key, Map<Key, BiConsumer>> subscribers(Bus<Key, ?> firehose) {
        return new Function<Key, Map<Key, BiConsumer>>() {
            @Override
            public Map<Key, BiConsumer> apply(Key key) {
                Map<Key, BiConsumer> consumers = new LinkedHashMap<>();

                Key currentKey = key;
                for (StreamSupplier supplier : suppliers) {
                    Key nextKey = currentKey.derive();
                    consumers.put(currentKey, supplier.get(currentKey, nextKey, firehose));
                    currentKey = nextKey;
                }
                return consumers;
            }
        };
    }

}
