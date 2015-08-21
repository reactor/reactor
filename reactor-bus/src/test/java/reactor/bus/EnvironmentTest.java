package reactor.bus;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.Environment;
import reactor.bus.selector.Selectors;
import reactor.core.support.Assert;
import reactor.fn.Consumer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class EnvironmentTest {

	protected Environment env;

	@Before
	public void loadEnv() {
		env = Environment.initializeIfEmpty().assignErrorJournal();
	}

	@After
	public void closeEnv() {
		Environment.terminate();
	}

	@Test
	public void testGetDispatcherThrowsExceptionWhenNoDispatcherIsFound() throws Exception {
		try {
			env.getDispatcher("NonexistingDispatcher");
			fail("Should have thrown an error");
		} catch (IllegalArgumentException e) {
			assertEquals("No Dispatcher found for name 'NonexistingDispatcher', it must be present in the " +
			  "configuration properties or being registered programmatically through this#setDispatcher" +
			  "(NonexistingDispatcher, someDispatcher)", e.getMessage());
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
