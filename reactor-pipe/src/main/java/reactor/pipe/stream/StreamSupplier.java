package reactor.pipe.stream;


import reactor.bus.Bus;
import reactor.fn.BiConsumer;
import reactor.pipe.key.Key;

@FunctionalInterface
public interface StreamSupplier<K extends Key, V> {
    BiConsumer<K, V> get(K src,
                         Key dst,
                         Bus<Key, Object> firehose);
}
