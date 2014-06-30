package reactor.util;

import org.junit.Before;
import org.junit.Test;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Jon Brisbin
 */
public class PartitionedReferencePileTests {

	int threadCnt = 8;

	ExecutorService pool;
	Timer           timer;

	volatile long now;

	@Before
	public void setup() {
		pool = Executors.newFixedThreadPool(threadCnt);
		timer = new Timer(true);

		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				now = System.currentTimeMillis();
			}
		}, 0, 200);
	}

	@Test
	public void testPartitionedReferencePileProvidesThreadLocal() throws InterruptedException {
		int backlog = 32;

		PartitionedReferencePile<Slot> pile = new PartitionedReferencePile<Slot>(backlog, Slot::new);
		CountDownLatch latch = new CountDownLatch(threadCnt);

		for (int i = 0; i < threadCnt; i++) {
			int idx = i;
			pool.submit(() -> {
				for (int j = 0; j < backlog * 2; j++) {
					Slot s = pile.get();
					s.var0 = idx;
					s.var1 = j;
				}

				int expected = 0;
				for (Slot s : pile) {
					assert s.var1 == expected++;
				}

				latch.countDown();
			});
		}

		assertThat("Latch was counted down", latch.await(5, TimeUnit.SECONDS));
	}

	@Test
	public void testPartitionedReferencePileThroughput() throws InterruptedException {
		int backlog = 64 * 1024;
		long start = System.nanoTime();

		PartitionedReferencePile<Slot> pile = new PartitionedReferencePile<Slot>(backlog, Slot::new);
		CountDownLatch latch = new CountDownLatch(threadCnt);
		for (int i = 0; i < threadCnt; i++) {
			int idx = i;
			pool.submit(() -> {
				for (int j = 0; j < backlog; j++) {
					Slot s = pile.get();
					s.var0 = idx;
					s.var1 = j;
				}

				for (Slot s : pile) {
					int slotIdx = s.var0;
				}
				latch.countDown();
			});
		}
		latch.await(5, TimeUnit.SECONDS);

		long end = System.nanoTime();
		double elapsed = TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS);
		System.out.println(String.format("throughput: %s ops/ms", (long) ((threadCnt * backlog) / elapsed)));
	}

	static class Slot {
		volatile int var0;
		volatile int var1;

		@Override
		public String toString() {
			return "Slot{" +
					"var0=" + var0 +
					", var1=" + var1 +
					'}';
		}
	}

}
