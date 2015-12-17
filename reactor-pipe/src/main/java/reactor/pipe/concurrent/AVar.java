package reactor.pipe.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Haskell-like Atomic Variable.
 *
 * @param <T> Type of value
 */
public class AVar<T> {

    private final CountDownLatch     latch;
    private final AtomicReference<T> ref;

    public AVar() {
        this(1);
    }

    public AVar(int i) {
        this.latch = new CountDownLatch(i);
        this.ref = new AtomicReference<T>();
    }

    public void set(T obj) {
        synchronized (this) {
            if (this.latch.getCount() > 0) {
                this.ref.set(obj);
                this.latch.countDown();
                return;
            }
        }
        throw new RuntimeException("This AVar has already been set");
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException {
        if (this.latch.await(timeout, unit)) {
            return this.ref.get();
        } else {
            throw new RuntimeException("AVar hasn't been set within a timeout");
        }
    }

    @Override
    public String toString() {
        return "AVar{" +
               "count=" + latch.getCount() +
               ", ref=" + ref +
               '}';
    }
}