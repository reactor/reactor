package pipe.pipe.stream;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongBinaryOperator;
import java.util.function.LongUnaryOperator;

public class FirehoseSubscription implements Subscription {

  private final AtomicLong           freeSlots;

  public FirehoseSubscription() {
    this.freeSlots = new AtomicLong(0);
  }

  @Override
  public void request(long l) {
    if (l < 1) {
      throw new RuntimeException("Can't request a non-positive number");
    }

    freeSlots.accumulateAndGet(l, new LongBinaryOperator() {
      @Override
      public long applyAsLong(long old, long diff) {
        long sum = old + diff;
        if (sum < 0 || sum == Long.MAX_VALUE) {
          return Long.MAX_VALUE; // Effectively unbounded
        } else {
          return sum;
        }
      }
    });
  }

  @Override
  public void cancel() {

  }



  public boolean maybeClaimSlot() {
    return freeSlots.getAndUpdate(new LongUnaryOperator() {
      @Override
      public long applyAsLong(long i) {
        if (i > 0) {
          return i - 1;
        } else {
          return 0;
        }
      }
    }) > 0;
  }

}
