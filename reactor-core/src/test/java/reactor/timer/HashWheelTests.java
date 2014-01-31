package reactor.timer;

import org.junit.Test;
import reactor.event.registry.Registration;
import reactor.function.Consumer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.Assert.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class HashWheelTests {

  private static int tolearance = 20;

  @Test
  public void submitForSingleExecution() throws InterruptedException {
    int delay = 500;
    HashWheelTimer timer = new HashWheelTimer();
    final CountDownLatch latch = new CountDownLatch(1);
    final long start = System.currentTimeMillis();
    final long[] elapsed = {0};

    timer.submit(new Consumer<Long>() {
      @Override
      public void accept(Long aLong) {
        latch.countDown();
        elapsed[0] = System.currentTimeMillis() - start;
      }
    }, delay, TimeUnit.MILLISECONDS);

    latch.await(1500, TimeUnit.MILLISECONDS);

    assertTrue(elapsed[0] >= delay);
    assertTrue(elapsed[0] < delay * 2);

  }

  @Test
  public void scheduleWithDelay() throws InterruptedException {
    HashWheelTimer timer = new HashWheelTimer(10, 512);

    final CountDownLatch latch10 = new CountDownLatch(49);
    final AtomicInteger count = new AtomicInteger(0);

    timer.schedule(new Consumer<Long>() {
      @Override
      public void accept(Long aLong) {
        latch10.countDown();
        count.incrementAndGet();
      }
    }, 10, TimeUnit.MILLISECONDS, 500);

    latch10.await(1000 + tolearance, TimeUnit.MILLISECONDS);
    timer.cancel();

    assertThat(latch10.getCount(), is(0L));
    assertThat(count.get(), is(49));
  }


  @Test
  public void tick100TimesEvery10Ms() throws InterruptedException {
    HashWheelTimer timer = new HashWheelTimer(10, 512);

    final CountDownLatch latch10 = new CountDownLatch(100);
    final AtomicInteger count = new AtomicInteger(0);

    timer.schedule(new Consumer<Long>() {
      @Override
      public void accept(Long aLong) {
        latch10.countDown();
        count.incrementAndGet();
      }
    }, 10, TimeUnit.MILLISECONDS);

    latch10.await(1000 + tolearance, TimeUnit.MILLISECONDS);
    timer.cancel();

    assertThat(latch10.getCount(), is(0L));
    assertThat(count.get(), is(100));
  }

  @Test
  public void tick100TimesEvery10MsPlusCancel() throws InterruptedException {
    HashWheelTimer timer = new HashWheelTimer(10, 512);

    final CountDownLatch latch10 = new CountDownLatch(50);
    final AtomicInteger count = new AtomicInteger(0);

    Registration r = timer.schedule(new Consumer<Long>() {
      @Override
      public void accept(Long _) {
        latch10.countDown();
        count.incrementAndGet();
      }
    }, 10, TimeUnit.MILLISECONDS);

    latch10.await(500 + tolearance, TimeUnit.MILLISECONDS);

    r.cancel();

    Thread.sleep(500);

    timer.cancel();

    assertThat(latch10.getCount(), is(0L));
    assertThat(count.get(), is(50));
  }

  @Test
  public void tick10TimesEvery100Ms() throws InterruptedException {
    HashWheelTimer timer = new HashWheelTimer(100, 512);

    final CountDownLatch latch100 = new CountDownLatch(10);
    final AtomicInteger count = new AtomicInteger(0);

    timer.schedule(new Consumer<Long>() {
      @Override
      public void accept(Long _) {
        latch100.countDown();
        count.incrementAndGet();
      }
    }, 100, TimeUnit.MILLISECONDS);

    latch100.await(1100 + tolearance, TimeUnit.MILLISECONDS);
    timer.cancel();

    assertThat(latch100.getCount(), is(0L));
    assertThat(count.get(), is(10));
  }

  @Test
  public void tick10TimesEvery100Ms2() throws InterruptedException {
    HashWheelTimer timer = new HashWheelTimer(10, 512);

    final CountDownLatch latch100 = new CountDownLatch(20);
    final AtomicInteger count = new AtomicInteger(0);

    timer.schedule(new Consumer<Long>() {
      @Override
      public void accept(Long _) {
        latch100.countDown();
        count.incrementAndGet();
      }
    }, 50, TimeUnit.MILLISECONDS);

    latch100.await(1100 + tolearance, TimeUnit.MILLISECONDS);
    timer.cancel();

    assertThat(latch100.getCount(), is(0L));
    assertThat(count.get(), is(20));
  }

  @Test
  public void pauseAndResumeTest() throws InterruptedException {
    HashWheelTimer timer = new HashWheelTimer(10, 512);

    final CountDownLatch latch10 = new CountDownLatch(100);
    final AtomicInteger count = new AtomicInteger(0);

    Registration r = timer.schedule(new Consumer<Long>() {
      @Override
      public void accept(Long aLong) {
        latch10.countDown();
        count.incrementAndGet();
      }
    }, 10, TimeUnit.MILLISECONDS);

    Thread.sleep(500);
    r.pause();
    Thread.sleep(500);

    latch10.await(500 + tolearance, TimeUnit.MILLISECONDS);
    timer.cancel();

    assertThat(latch10.getCount(), is(0L));
    assertThat(count.get(), is(100));
  }
}
