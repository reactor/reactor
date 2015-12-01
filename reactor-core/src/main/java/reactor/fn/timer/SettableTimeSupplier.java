package reactor.fn.timer;

import reactor.fn.LongSupplier;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * Settable Time Supplier that could be used for Testing purposes or
 * in systems where time doesn't correspond to the wall clock.
 */
public class SettableTimeSupplier implements LongSupplier {

    private volatile long                                         current;
    private final    AtomicLongFieldUpdater<SettableTimeSupplier> fieldUpdater;
    private final    AtomicBoolean                                initialValueRead;
    private final    long                                         initialTime;


    public SettableTimeSupplier(long initialTime) {
        this.initialValueRead = new AtomicBoolean(false);
        this.initialTime = initialTime;
        this.fieldUpdater = AtomicLongFieldUpdater.newUpdater(SettableTimeSupplier.class, "current");
    }

    @Override
    public long get() {
        if (initialValueRead.get()) {
            return current;
        } else {
            initialValueRead.set(true);
            return initialTime;
        }

    }

    public void set(long newCurrent) throws InterruptedException {
        this.fieldUpdater.set(this, newCurrent);
    }
}