package reactor.core.processor;

import org.junit.Test;
import reactor.core.support.Assert;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author Anatoly Kadyshev
 */
public class SharedProcessorServiceTests {

	private final int           BUFFER_SIZE     = 8;
	private final AtomicBoolean exceptionThrown = new AtomicBoolean();
	;
	private final int N = 17;

	@Test
	public void testDispatch() throws InterruptedException {
		SharedProcessorService<String> service = SharedProcessorService.async("dispatcher", BUFFER_SIZE, t -> {
			exceptionThrown.set(true);
			t.printStackTrace();
		});
		BiConsumer<String, Consumer<? super String>> dispatcher = service.dataDispatcher();

		runTest(dispatcher);
		runTest(dispatcher);
	}

	private void runTest(final BiConsumer<String, Consumer<? super String>> dispatcher) throws InterruptedException {
		CountDownLatch tasksCountDown = new CountDownLatch(N);

		dispatcher.accept("Hello", s -> {
			for (int i = 0; i < N; i++) {
				dispatcher.accept("world", s1 -> tasksCountDown.countDown());
			}
		});

		Assert.isTrue(tasksCountDown.await(5, TimeUnit.SECONDS));
		Assert.isTrue(!exceptionThrown.get());
	}

}