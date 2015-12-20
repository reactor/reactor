package reactor.pipe;

import org.junit.Test;
import reactor.core.processor.RingBufferWorkProcessor;
import reactor.pipe.key.Key;
import reactor.pipe.registry.ConcurrentRegistry;
import reactor.pipe.router.NoOpRouter;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@SuppressWarnings("unchecked")
public abstract class AbstractPipeTests extends AbstractRawBusTests {

    protected abstract <T, O> void subscribe(IPipe.IPipeEnd<T, O> pipe);

    protected abstract <T, O> void subscribeAndDispatch(IPipe.IPipeEnd<T, O> pipe, List<T> value);

    @Test
    public void testSmoke() throws InterruptedException { // Tests backpressure and in-thread dispatches
        RingBufferWorkProcessor<Runnable> processor = RingBufferWorkProcessor.<Runnable>create(
                Executors.newFixedThreadPool(4),
                256);
        RawBus<Key, Object> bus = new RawBus<Key, Object>(new ConcurrentRegistry<>(),
                processor,
                4,
                new NoOpRouter<>(),
                null,
                null);


        int iterations = 2000;
        CountDownLatch latch = new CountDownLatch(iterations);

        Pipe.<Integer>build()
                .map((i) -> i + 1)
                .map(i -> {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    return i * 2;
                })
                .consume((i_) -> {
                    latch.countDown();
                }).subscribe(Key.wrap("source", "first"), bus);

        for (int i = 0; i < iterations; i++) {
            bus.notify(Key.wrap("source", "first"), i);
            if (i % 500 == 0) {
                System.out.println("Processed " + i + " keys");
            }
        }

        latch.await(5, TimeUnit.MINUTES);
        assertThat(latch.getCount(), is(0L));

        processor.shutdown();
    }
}
