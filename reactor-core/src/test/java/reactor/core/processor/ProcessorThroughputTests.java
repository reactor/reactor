package reactor.core.processor;

import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.function.Consumer;
import reactor.function.Supplier;

/**
 * @author Jon Brisbin
 */
public class ProcessorThroughputTests {

	static final int RUNS = 100000;

	Processor<Data> processor;
	CountDownLatch  latch;
	Consumer<Data>  dataConsumer;
	long            start;

	@Before
	public void setup() {
		latch = new CountDownLatch(RUNS);

		dataConsumer = new Consumer<Data>() {
			@Override public void accept(Data data) {
				data.type = "test";
			}
		};

		processor = new Processor<Data>(
				new Supplier<Data>() {
					@Override public Data get() {
						return new Data();
					}
				},
				new Consumer<Data>() {
					@Override public void accept(Data data) {
						latch.countDown();
					}
				},
				null
		);

		start = System.currentTimeMillis();
	}

	@After
	public void cleanup() {
		long end = System.currentTimeMillis();
		long elapsed = (end - start);
		long throughput = Math.round(RUNS / ((double)elapsed / 1000));
		System.out.println("elapsed: " + elapsed + "ms, throughput: " + throughput + "/sec");
	}

	@Test
	public void testProcessorThroughput() throws InterruptedException {
		int batchSize = 256;
		int runs = RUNS / batchSize;

		for(int i = 0; i < runs; i++) {
			processor.batch(batchSize, dataConsumer);
		}

		assertTrue(latch.await(60, TimeUnit.SECONDS));
	}

	static final class Data {
		String type;
	}

}
