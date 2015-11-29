package reactor.pipe.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import reactor.fn.Supplier;

public class LazyVar<T> {

  private final AtomicReference<T> ref;
  private final AtomicBoolean      isSet;
  private final CountDownLatch     latch;
  private final Supplier<T>        supplier;

  public LazyVar(Supplier<T> supplier) {
    this.supplier = supplier;
    this.ref = new AtomicReference<>();
    this.isSet = new AtomicBoolean();
    this.latch = new CountDownLatch(1);
  }

  public T get() {
    if (isSet.compareAndSet(false, true)) {
      this.ref.set(supplier.get());
      this.latch.countDown();
    }

    // We need a latch here because otherwise we can't guarantee concurrent getters not
    // to race with setter.
    try {
      latch.await();
    } catch (InterruptedException e) {
      // hand a control back over
    }
    return this.ref.get();
  }
}
