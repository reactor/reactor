package reactor.pipe;

import org.junit.After;
import org.junit.Before;
import reactor.bus.AbstractBus;
import reactor.bus.Bus;
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.pipe.key.Key;
import reactor.pipe.registry.ConcurrentRegistry;
import reactor.pipe.router.NoOpRouter;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AbstractRawBusTests {

    public static final long LATCH_TIMEOUT = 10;
    public static final TimeUnit LATCH_TIME_UNIT = TimeUnit.SECONDS;

    protected AbstractBus<Key, Object> firehose;

    @Before
    public void setup() {

        this.firehose = new RawBus<Key, Object>(new ConcurrentRegistry<>(),
                RingBufferWorkProcessor.<Runnable>create(Executors.newFixedThreadPool(1),
                        1024),
                1,
                new NoOpRouter<>(),
                null,
                null);
    }

    @After
    public void teardown() {
        //firehose.shutdown();
    }

}
