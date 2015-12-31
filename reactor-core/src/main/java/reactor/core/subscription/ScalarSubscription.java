package reactor.core.subscription;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.support.BackpressureUtils;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public final class ScalarSubscription<T> implements Subscription {

    final Subscriber<? super T> actual;

    final T value;

    volatile int once;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<ScalarSubscription> ONCE =
      AtomicIntegerFieldUpdater.newUpdater(ScalarSubscription.class, "once");

    public ScalarSubscription(Subscriber<? super T> actual, T value) {
        this.value = Objects.requireNonNull(value, "value");
        this.actual = Objects.requireNonNull(actual, "actual");
    }

    @Override
    public void request(long n) {
        if (BackpressureUtils.validate(n)) {
            if (ONCE.compareAndSet(this, 0, 1)) {
                Subscriber<? super T> a = actual;
                a.onNext(value);
                a.onComplete();
            }
        }
    }

    @Override
    public void cancel() {
        ONCE.lazySet(this, 1);
    }
}
