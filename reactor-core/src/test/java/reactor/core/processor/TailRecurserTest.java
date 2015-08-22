package reactor.core.processor;

import org.hamcrest.Matchers;
import org.junit.Test;
import reactor.core.processor.SharedProcessorService;
import reactor.fn.Consumer;
import reactor.fn.Supplier;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertThat;

/**
 * @author Anatoly Kadyshev
 */
public class TailRecurserTest {

	@Test
	public void testConsumeTasks() throws Exception {
		AtomicInteger nRecursiveTasks = new AtomicInteger(0);

		Consumer<SharedProcessorService.Task> taskConsumer = new Consumer<SharedProcessorService.Task>() {
			@Override
			public void accept(SharedProcessorService.Task dispatcherTask) {
				nRecursiveTasks.incrementAndGet();
			}
		};

		SharedProcessorService.TailRecurser recursion = new SharedProcessorService.TailRecurser(1,
		  new Supplier<SharedProcessorService.Task>() {
			  @Override
			  public SharedProcessorService.Task get() {
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