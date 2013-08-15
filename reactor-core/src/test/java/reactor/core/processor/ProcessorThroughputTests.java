package reactor.core.processor;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import reactor.function.Consumer;
import reactor.function.Supplier;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * @author Jon Brisbin
 */
@Ignore
public class ProcessorThroughputTests {

	static final int RUNS = 500000000;

	Processor<Data> proc;
	CountDownLatch  latch;
	Supplier<Data> dataSupplier = new Supplier<Data>() {
		@Override
		public Data get() {
			return new Data();
		}
	};
	Consumer<Data> dataConsumer;
	long           start;

	@SuppressWarnings("unchecked")
	@Before
	public void setup() {
		latch = new CountDownLatch(RUNS / 2);

		dataConsumer = new Consumer<Data>() {
			@Override
			public void accept(Data data) {
				//data.type = "test";
			}
		};

		Consumer<Data> countDownConsumer = new Consumer<Data>() {
			@Override
			public void accept(Data data) {
				latch.countDown();
			}
		};

		proc = new reactor.core.processor.spec.ProcessorSpec<Data>()
				.dataSupplier(dataSupplier)
						//.dataBufferSize(1024 * 4)
						//.consume(new DelegatingConsumer<Data>().add(countDownConsumer))
				.consume(countDownConsumer)
				.get();

		start = System.currentTimeMillis();
	}

	@After
	public void cleanup() {
		long end = System.currentTimeMillis();
		long elapsed = (end - start);
		long throughput = Math.round(RUNS / ((double) elapsed / 1000));
		System.out.println("elapsed: " + elapsed + "ms, throughput: " + throughput + "/sec");
	}

	@Test
	public void testProcessorThroughput() throws InterruptedException {
		int batchSize = 512;
		int runs = RUNS / batchSize;

		for (int i = 0; i < runs; i++) {
			proc.batch(batchSize, dataConsumer);
		}

		assertTrue(latch.await(60, TimeUnit.SECONDS));
	}

	static final class Data {
		String type;
		Long   run;
	}

}
