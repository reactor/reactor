package reactor.pipe;

import org.junit.After;
import org.junit.Before;
import org.pcollections.TreePVector;

import reactor.Timers;
import reactor.bus.AbstractBus;
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.core.timer.Timer;
import reactor.fn.Supplier;
import reactor.pipe.key.Key;
import reactor.pipe.registry.ConcurrentRegistry;
import reactor.pipe.router.NoOpRouter;
import reactor.pipe.state.DefaultStateProvider;
import reactor.pipe.stream.StreamSupplier;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AbstractRawBusTests {

    public static final long     LATCH_TIMEOUT   = 10;
    public static final TimeUnit LATCH_TIME_UNIT = TimeUnit.SECONDS;

    protected AbstractBus<Key, Object>          firehose;
    protected RingBufferWorkProcessor<Runnable> processor;
    protected Pipe<Integer, Integer> integerPipe;

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
        this.integerPipe = new Pipe<Integer, Integer>(TreePVector.<StreamSupplier>empty(), new DefaultStateProvider<Key>(), new Supplier<Timer>() {
            @Override
            public Timer get() {
                // for tests we use a higher resolution timer
                return Timers.create("pipe-timer", 1, 512);
            }
      });
    }

    @After
    public void teardown() {
        processor.shutdown();
    }

}
