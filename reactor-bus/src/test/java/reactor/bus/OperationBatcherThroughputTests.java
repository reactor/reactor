package reactor.bus;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import reactor.bus.batcher.OperationBatcher;
import reactor.bus.batcher.spec.OperationBatcherSpec;
import reactor.fn.Consumer;
import reactor.fn.Supplier;

/**
 * @author Jon Brisbin
 */
@Ignore
public class OperationBatcherThroughputTests {

	static final int RUNS = 250000000;
    static final int BATCH_SIZE = 512;

    static final class Data {
        String type;
        Long   run;
    }

	private static final Supplier<Data> dataSupplier = new Supplier<Data>() {
		@Override
		public Data get() {
			return new Data();
		}
	};

    private static final Consumer<Data> dataConsumer = new Consumer<Data>() {
        @Override
        public void accept(Data data) {
            data.type = "test";
        }

    };

    private static final Consumer<Data> nullConsumer= new Consumer<Data>() {
        @Override
        public void accept(Data data) {
        }
    };

	long           start;
    String         waitStrategy;

	@Before
	public void setup() {
	}

	@After
	public void cleanup() {
		long end = System.currentTimeMillis();
		long elapsed = (end - start);
		long throughput = Math.round(RUNS / ((double) elapsed / 1000));
		System.out.println(waitStrategy + " > elapsed: " + elapsed + "ms, throughput: " + throughput + "/sec");
	}

	@Test
	public void testProcessorThroughput() throws InterruptedException {
        OperationBatcher<Data> proc = new OperationBatcherSpec<Data>()
            .dataSupplier(dataSupplier)
            .consume(nullConsumer)
            .blockingWaitStrategy()
            .get();

        waitStrategy = "default";
        start = System.currentTimeMillis();

		int runs = RUNS / BATCH_SIZE;

		for (int i = 0; i < runs; i++) {
			proc.batch(BATCH_SIZE, dataConsumer);
		}
	}

    @Test
    public void testBlockingWaitStrategyThroughput() throws InterruptedException {
        OperationBatcher<Data> proc = new OperationBatcherSpec<Data>()
            .dataSupplier(dataSupplier)
            .consume(nullConsumer)
            .blockingWaitStrategy()
            .get();

        waitStrategy = "blockingWaitStrategy";
        start = System.currentTimeMillis();

        int runs = RUNS / BATCH_SIZE;
        for (int i = 0; i < runs; i++) {
            proc.batch(BATCH_SIZE, dataConsumer);
        }
    }

    @Test
    public void testBusySpinWaitStrategyThroughput() throws InterruptedException {
        OperationBatcher<Data> proc = new OperationBatcherSpec<Data>()
            .dataSupplier(dataSupplier)
            .consume(nullConsumer)
            .busySpinWaitStrategy()
            .get();

        waitStrategy = "busySpinWaitStrategy";
        start = System.currentTimeMillis();

        int runs = RUNS / BATCH_SIZE;
        for (int i = 0; i < runs; i++) {
            proc.batch(BATCH_SIZE, dataConsumer);
        }
    }

    @Test
    public void testSleepingWaitStrategyThroughput() throws InterruptedException {
        OperationBatcher<Data> proc = new OperationBatcherSpec<Data>()
            .dataSupplier(dataSupplier)
            .consume(nullConsumer)
            .sleepingWaitStrategy()
            .get();

        waitStrategy = "sleepingWaitStrategy";
        start = System.currentTimeMillis();

        int runs = RUNS / BATCH_SIZE;
        for (int i = 0; i < runs; i++) {
            proc.batch(BATCH_SIZE, dataConsumer);
        }
    }

    @Test
    public void testYieldingWaitStrategyThroughput() throws InterruptedException {
        OperationBatcher<Data> proc = new OperationBatcherSpec<Data>()
            .dataSupplier(dataSupplier)
            .consume(nullConsumer)
            .yieldingWaitStrategy()
            .get();

        waitStrategy = "yieldingWaitStrategy";
        start = System.currentTimeMillis();

        int runs = RUNS / BATCH_SIZE;
        for (int i = 0; i < runs; i++) {
            proc.batch(BATCH_SIZE, dataConsumer);
        }
    }

}
