package reactor.core.dispatch;

import org.junit.Test;
import reactor.fn.Consumer;
import reactor.jarjar.com.lmax.disruptor.BusySpinWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Anatoly Kadyshev
 */
public class RingBufferDispatcher3Test {

	private final int BUFFER_SIZE = 8;

	private final int N = 17;

	@Test
	public void testDispatch() throws InterruptedException {
		RingBufferDispatcher3 dispatcher = new RingBufferDispatcher3("dispatcher", BUFFER_SIZE, null, ProducerType
		  .MULTI,
		  new BusySpinWaitStrategy());

		runTest(dispatcher);
		runTest(dispatcher);
	}

	private void runTest(final RingBufferDispatcher3 dispatcher) throws InterruptedException {
		CountDownLatch tasksCountDown = new CountDownLatch(N);
		AtomicBoolean exceptionThrown = new AtomicBoolean();
		dispatcher.dispatch("Hello", new Consumer<String>() {
			@Override
			public void accept(String s) {
				for (int i = 0; i < N; i++) {
					dispatcher.dispatch("world", new Consumer<String>() {
						@Override
						public void accept(String s) {
							tasksCountDown.countDown();
						}
					}, null);
				}
			}
		}, new Consumer<Throwable>() {
			@Override
			public void accept(Throwable t) {
				exceptionThrown.set(true);
				t.printStackTrace();
			}
		});

		assertTrue(tasksCountDown.await(5, TimeUnit.SECONDS));
		assertFalse(exceptionThrown.get());
	}

}