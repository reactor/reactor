package reactor.pipe;

import org.junit.After;
import org.junit.Before;
import reactor.bus.AbstractBus;
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.pipe.key.Key;
import reactor.pipe.registry.ConcurrentRegistry;
import reactor.pipe.router.NoOpRouter;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AbstractRawBusTests {

    public static final long     LATCH_TIMEOUT   = 10;
    public static final TimeUnit LATCH_TIME_UNIT = TimeUnit.SECONDS;

    protected AbstractBus<Key, Object>          firehose;
    protected RingBufferWorkProcessor<Runnable> processor;

    @Before
    public void setup() {
        this.processor = RingBufferWorkProcessor.<Runnable>create(Executors.newFixedThreadPool(1),
                                                                  1024);
        this.firehose = new RawBus<Key, Object>(new ConcurrentRegistry<>(),
                                                processor,
                                                1,
                                                new NoOpRouter<>(),
                                                null,
                                                null);
    }

    @After
    public void teardown() {
        processor.shutdown();
    }

}
