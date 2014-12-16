package reactor.core.support;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

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
	public void partitionedReferencePileProvidesThreadLocal() throws InterruptedException {
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

		MatcherAssert.assertThat("Latch was counted down", latch.await(5, TimeUnit.SECONDS));
	}

	@Test
	public void partitionedReferencePileCollectsAllObjects() throws InterruptedException, ExecutionException {
		int backlog = 32;

		PartitionedReferencePile<Slot> pile = new PartitionedReferencePile<Slot>(backlog, Slot::new);

		Future[] futures = new Future[threadCnt];
		for (int i = 0; i < threadCnt; i++) {
			int idx = i;
			futures[i] = pool.submit(() -> {
				for (int j = 0; j < backlog * 2; j++) {
					Slot s = pile.get();
					s.var0 = idx;
					s.var1 = j;
				}
			});
		}
		for (Future f : futures) {
			f.get();
		}

		Set<Slot> slots = pile.collect().castToSet();

		MatcherAssert.assertThat("All Slots allocated are available", slots, Matchers.iterableWithSize(threadCnt * backlog
				* 2));
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
