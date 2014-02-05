package reactor.dispatch;

import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.Test;
import reactor.event.dispatch.RingBufferDispatcher;
import reactor.event.dispatch.WorkQueueDispatcher;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jon Brisbin
 */
public class ExecutorThroughputTests {

	static int runs        = 1000000;
	static int threadCount = 8;

	private void doTest(Executor executor) {
		final AtomicLong count = new AtomicLong();
		Runnable r = new Runnable() {
			@Override
			public void run() {
				count.incrementAndGet();
			}
		};
		long start = System.currentTimeMillis();
		for(int i = 0; i < runs; i++) {
			executor.execute(r);
		}
		long end = System.currentTimeMillis();
		double elapsed = end - start;
		int throughput = (int)(count.get() / (elapsed / 1000));

		System.out.println(executor.getClass().getSimpleName() + " throughput: " + throughput + "/sec");
	}

	@Test
	public void testSingleThreadThreadPoolExecutorThroughput() {
		Executor executor = Executors.newFixedThreadPool(1);

		doTest(executor);
	}

	@Test
	public void testMultiThreadThreadPoolExecutorThroughput() {
		Executor executor = Executors.newFixedThreadPool(threadCount);

		doTest(executor);
	}

	@Test
	public void testRingBufferExecutorThroughput() {
		Executor executor = new RingBufferDispatcher("ringBufferExecutor",
		                                             2048,
		                                             null,
		                                             ProducerType.SINGLE,
		                                             new YieldingWaitStrategy());

		doTest(executor);
	}

	@Test
	public void testWorkQueueExecutorThroughput() {
		Executor executor = new WorkQueueDispatcher("workQueueExecutor", threadCount, 2048, null);

		doTest(executor);
	}

}