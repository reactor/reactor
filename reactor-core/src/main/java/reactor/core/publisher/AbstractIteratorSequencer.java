package reactor.core.publisher;

import reactor.core.subscriber.SubscriberWithContext;
import reactor.fn.Consumer;

import java.util.Iterator;

public abstract class AbstractIteratorSequencer<T>
        implements Consumer<SubscriberWithContext<T, Iterator<? extends T>>> {

    @Override
    public final void accept(SubscriberWithContext<T, Iterator<? extends T>> subscriber) {
        final Iterator<? extends T> iterator = subscriber.context();
        if (iterator.hasNext()) {
            subscriber.onNext(iterator.next());
        }
        else {
            subscriber.onComplete();
        }
    }

}
