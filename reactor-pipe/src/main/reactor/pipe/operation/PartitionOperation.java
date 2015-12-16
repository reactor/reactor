package reactor.pipe.operation;

import org.pcollections.PVector;
import org.pcollections.TreePVector;
import reactor.bus.Bus;
import reactor.fn.BiConsumer;
import reactor.fn.Predicate;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;

import java.util.List;

public class PartitionOperation<SRC extends Key, DST extends Key, V> implements BiConsumer<SRC, V> {

    private final Atom<PVector<V>>   buffer;
    private final Bus<Key, Object>   firehose;
    private final Predicate<List<V>> emit;
    private final DST                destination;

    public PartitionOperation(Bus<Key, Object> firehose,
                              Atom<PVector<V>> buffer,
                              Predicate<List<V>> emit,
                              DST destination) {
        this.buffer = buffer;
        this.firehose = firehose;
        this.emit = emit;
        this.destination = destination;
    }

    @Override
    @SuppressWarnings(value = {"unchecked"})
    public void accept(SRC key, V value) {
        PVector<V> newv = buffer.update((old) -> old.plus(value));

        if (emit.test(newv)) {
            PVector<V> downstreamValue = buffer.updateAndReturnOld((old) -> TreePVector.empty());
            firehose.notify(destination.clone(key), downstreamValue);
        }
    }
}
