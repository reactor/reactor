package reactor.pipe.router;

import reactor.bus.registry.Registration;
import reactor.bus.routing.Router;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;

import java.util.List;

public class NoOpRouter<K, V> implements Router<K, V> {

    @SuppressWarnings("unchecked")
    @Override
    public <E extends V> void route(K key, E data,
                                    List<Registration<K, ? extends BiConsumer<K, ? extends V>>> consumers,
                                    Consumer<E> completionConsumer,
                                    Consumer<Throwable> errorConsumer) {
        for (Registration<K, ? extends BiConsumer<K, ? extends V>> reg : consumers) {
            ((BiConsumer<K, E>) reg.getObject()).accept(key, data);
        }
    }
}
