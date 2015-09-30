package reactor.core.processor;

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
	public void testConsumeTasks() throws Exception {
		AtomicInteger nRecursiveTasks = new AtomicInteger(0);

		Consumer<ProcessorGroup.Task> taskConsumer = new Consumer<ProcessorGroup.Task>() {
			@Override
			public void accept(ProcessorGroup.Task dispatcherTask) {
				nRecursiveTasks.incrementAndGet();
			}
		};

		ProcessorGroup.TailRecurser recursion = new ProcessorGroup.TailRecurser(1,
		  new Supplier<ProcessorGroup.Task>() {
			  @Override
			  public ProcessorGroup.Task get() {
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