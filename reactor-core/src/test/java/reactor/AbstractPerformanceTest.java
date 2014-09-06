package reactor;

import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Environment;
import reactor.jarjar.jsr166e.LongAdder;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Jon Brisbin
 */
public abstract class AbstractPerformanceTest {

	protected static final Timer TIMER = new Timer();
	protected static volatile long now;

	static {
		TIMER.schedule(
				new TimerTask() {
					@Override
					public void run() {
						now = System.currentTimeMillis();
					}
				},
				0,
				200
		);
	}

	private final Logger log = LoggerFactory.getLogger(getClass());

	protected ExecutorService pool;
	protected Environment     env;
	protected LongAdder       counter;
	protected long            start;

	@Before
	public void setup() {
		pool = Executors.newCachedThreadPool();
		counter = new LongAdder();
		env = new Environment();
	}

	protected long getTimeout() {
		return 1000;
	}

	protected void startThroughputTest(String type) {
		log.info("Starting {} throughput test...", type);
		start = System.currentTimeMillis();
	}

	protected void stopThroughputTest(String type) {
		long end = System.currentTimeMillis();
		double elapsed = end - start;
		long throughput = (long) (counter.longValue() / (elapsed / 1000));

		log.info("{} throughput: {}/s in {}ms", type, throughput, (int) elapsed);
	}

	protected boolean withinTimeout() {
		return now - start < getTimeout();
	}

	protected void fork(int threadCount, Runnable r) throws ExecutionException, InterruptedException {
		Future[] futures = new Future[threadCount];
		for (int i = 0; i < threadCount; i++) {
			futures[i] = pool.submit(r);
		}
		for (Future f : futures) {
			if (null != f) {
				f.get();
			}
		}
	}

}
