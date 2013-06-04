package reactor.logback;

import ch.qos.logback.classic.Logger;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
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

	@Test
	public void asyncAppenderLogsAsynchronously() {
		Logger log = (Logger) LoggerFactory.getLogger(AsyncAppenderTests.class);

		log.info(MSG);

		assertThat("Appender is not set", log.iteratorForAppenders().next(), instanceOf(AsyncAppender.class));
	}

	private long benchmarkThread(final Logger logger, int timeout) throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicLong i = new AtomicLong(0);

		for (int j = 0;  j < Runtime.getRuntime().availableProcessors() - 1; j++) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					while (latch.getCount() > 0) {
						logger.warn("" + i.incrementAndGet());
					}
				}
			}).start();
		}

		latch.await(timeout, TimeUnit.SECONDS);
		latch.countDown();
		return i.get();
	}

	@Test
	@Ignore
	public void benchmark() throws InterruptedException {

		int timeout = 20;
		long m = benchmarkThread((Logger) LoggerFactory.getLogger(reactor.dummy.Test.class), timeout);
		long n = benchmarkThread((Logger) LoggerFactory.getLogger(AsyncAppenderTests.class), timeout);

		System.out.println(n + " " + m);

	}

}
