package reactor.core;

import org.junit.Test;
import reactor.AbstractReactorTest;
import reactor.Environment;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.bus.selector.Selectors;
import reactor.core.support.Assert;
import reactor.fn.Consumer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class EnvironmentTest extends AbstractReactorTest {

    @Test
    public void testGetDispatcherThrowsExceptionWhenNoDispatcherIsFound() throws Exception {
        try {
            env.getDispatcher("NonexistingDispatcher");
            fail("Should have thrown an exception");
        } catch ( IllegalArgumentException e ) {
            assertEquals("No Dispatcher found for name 'NonexistingDispatcher'", e.getMessage());
        }
    }

	@Test
	public void workerOrchestrator() throws InterruptedException {
		EventBus reactor = EventBus.create(env, Environment.WORK_QUEUE);

		CountDownLatch latch = new CountDownLatch(3);

		reactor.on(Selectors.$("worker"), o -> {
				System.out.println(Thread.currentThread().getName() + " worker " + o);
				reactor.notify("orchestrator", Event.wrap(1000));
				latch.countDown();
				System.out.println(Thread.currentThread().getName() + " ok");
		});

		reactor.on(Selectors.$("orchestrator"), new Consumer<Event<Integer>>() {

			@Override
			public void accept(Event<Integer> event) {
				sendTask();
			}

			void sendTask() {
				System.out.println(Thread.currentThread().getName() + " sendTask ");
				reactor.notify("worker", Event.wrap(latch.getCount()));
				latch.countDown();
			}
		});

		reactor.notify("orchestrator", Event.wrap(1000));

		Assert.isTrue(latch.await(10, TimeUnit.SECONDS));
	}

}
