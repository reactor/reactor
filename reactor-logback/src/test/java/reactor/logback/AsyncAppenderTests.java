package reactor.logback;

import ch.qos.logback.classic.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jon Brisbin
 */
public class AsyncAppenderTests {

	static final String MSG;

	static {
		String ABCS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
		Random r = new Random();

		char[] chars = new char[20000];
		int len = chars.length;
		for (int i = 0; i < len; i++) {
			chars[i] = ABCS.charAt(r.nextInt(ABCS.length()));
		}
		MSG = new String(chars);
	}

	final int timeout = 5;
	ExecutorService threadPool;

	@Before
	public void setup() {
		threadPool = Executors.newCachedThreadPool();
	}

	@After
	public void cleanup() {
		threadPool.shutdownNow();
	}

	@Test
	public void clockAsyncAppender() throws InterruptedException {
		long n = benchmarkThread((Logger) LoggerFactory.getLogger("reactor"), timeout);
		System.out.println("async: " + (n / timeout) + "/sec");
	}

	@Test
	public void clockSyncAppender() throws InterruptedException {
		long m = benchmarkThread((Logger) LoggerFactory.getLogger("sync"), timeout);
		System.out.println("sync: " + (m / timeout) + "/sec");
	}

	@Ignore
	@Test
	public void clockBothAppenders() throws InterruptedException {
		clockSyncAppender();
		clockAsyncAppender();
	}

	private long benchmarkThread(final Logger logger, int timeout) throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicLong throughput = new AtomicLong(0);

		int threads = Runtime.getRuntime().availableProcessors();
		for (int i = 0; i < threads; i++) {
			threadPool.submit(new Runnable() {
				@Override
				public void run() {
					while (latch.getCount() > 0) {
						logger.warn("" + throughput.incrementAndGet());
					}
				}
			});
		}

		latch.await(timeout, TimeUnit.SECONDS);
		latch.countDown();
		return throughput.get();
	}

}
