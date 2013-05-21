package reactor.fn;

import static org.junit.Assert.assertEquals;
import static reactor.Fn.$;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import reactor.Fn;
import reactor.core.Reactor;
import reactor.fn.dispatch.SynchronousDispatcher;

public class RegistrationTests {

	@Test
	public void registrationsCanBeUsedAtMostOnce() throws InterruptedException {
		final String key = "key";
		final Selector selector = $(key);

		final Reactor reactor = new Reactor(new SynchronousDispatcher());

		Thread notifyThread = new Thread(new Runnable() {
			public void run() {
				while (true) {
					reactor.notify(key, Fn.event("hello"));
				}
			}
		});

		notifyThread.start();


		for (int i = 0; i < 10000; i++) {
			AcceptCountingConsumer consumer = new AcceptCountingConsumer();
			reactor.on(selector, consumer).cancelAfterUse();
			consumer.await();
			assertEquals(1, consumer.acceptCount.getAndSet(0));
		}
	}

	private static final class AcceptCountingConsumer implements Consumer<Event<String>> {

		private final AtomicInteger acceptCount = new AtomicInteger();

		private final CountDownLatch latch = new CountDownLatch(1);

		@Override
		public void accept(Event<String> t) {
			acceptCount.incrementAndGet();
			latch.countDown();
		}

		private void await() throws InterruptedException {
			latch.await();
		}
	}

}
