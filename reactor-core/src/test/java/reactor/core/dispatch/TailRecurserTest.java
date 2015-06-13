package reactor.core.dispatch;

import org.hamcrest.Matchers;
import org.junit.Test;
import reactor.fn.Consumer;
import reactor.fn.Supplier;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertThat;

/**
 * @author Anatoly Kadyshev
 */
public class TailRecurserTest {

    @Test
    public void testDefaultTailRecurser() throws Exception {
        AtomicInteger nRecursiveTasks = new AtomicInteger(0);

        Consumer<RingBufferDispatcher3.Task> taskConsumer = new Consumer<RingBufferDispatcher3.Task>() {
            @Override
            public void accept(RingBufferDispatcher3.Task dispatcherTask) {
                nRecursiveTasks.incrementAndGet();
            }
        };

        RingBufferDispatcher3.TailRecurser recursion = new RingBufferDispatcher3.TailRecurser(1,
                new Supplier<RingBufferDispatcher3.Task>() {
            @Override
            public RingBufferDispatcher3.Task get() {
                return null;
            }
        }, taskConsumer);

        recursion.next();
        recursion.next();

        recursion.consumeTasks();

        assertThat(nRecursiveTasks.get(), Matchers.is(2));

        nRecursiveTasks.set(0);

        recursion.next();
        recursion.next();
        recursion.next();

        recursion.consumeTasks();

        assertThat(nRecursiveTasks.get(), Matchers.is(3));
    }

}