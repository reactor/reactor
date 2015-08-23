package reactor.core.processor;

import org.junit.Test;
import org.reactivestreams.Processor;
import org.testng.SkipException;
import reactor.core.support.Assert;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author Anatoly Kadyshev
 * @author Stephane Maldini
 */
@org.testng.annotations.Test
public class SharedProcessorServiceAsyncTests extends AbstractProcessorTests {

	private final int           BUFFER_SIZE     = 8;
	private final AtomicBoolean exceptionThrown = new AtomicBoolean();
	private final int N = 17;

	@Override
	public Processor<Long, Long> createIdentityProcessor(int bufferSize) {
		return SharedProcessorService.<Long>async("tckRingBufferProcessor", bufferSize).get();
	}

	@Override
	public void required_spec104_mustCallOnErrorOnAllItsSubscribersIfItEncountersANonRecoverableError() throws
	  Throwable {
		throw new SkipException("Optional requirement");
	}

	@Test
	public void testDispatch() throws InterruptedException {
		SharedProcessorService<String> service = SharedProcessorService.async("dispatcher", BUFFER_SIZE, t -> {
			exceptionThrown.set(true);
			t.printStackTrace();
		});
		BiConsumer<String, Consumer<? super String>> dispatcher = service.dataDispatcher();

		runTest(dispatcher);
		runTest(dispatcher);

		SharedProcessorService.release(dispatcher);
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