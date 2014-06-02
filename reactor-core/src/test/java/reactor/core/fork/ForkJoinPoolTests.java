package reactor.core.fork;

import com.gs.collections.api.list.ImmutableList;
import com.gs.collections.impl.list.mutable.FastList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Environment;
import reactor.function.Function;
import reactor.rx.Promise;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * @author Jon Brisbin
 */
public class ForkJoinPoolTests {

	Environment  env;
	ForkJoinPool fjp;

	@Before
	public void setup() {
		env = new Environment();
		fjp = new ForkJoinPool(env);
	}

	@After
	public void cleanup() {
		env.shutdown();
	}

	@Test
	public void forkJoinPoolJoinsTasks() throws InterruptedException {
		int runs = 100;
		Function<Void, Integer> task = new Function<Void, Integer>() {
			final AtomicInteger count = new AtomicInteger(0);
			final Random random = new Random(System.nanoTime());

			@Override
			public Integer apply(Void v) {
				try {
					Thread.sleep(random.nextInt(500));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("thread=" + Thread.currentThread().getName());
				return count.incrementAndGet();
			}
		};

		List<Function<?, Integer>> tasks = FastList.newList();
		for (int i = 0; i < runs; i++) {
			tasks.add(task);
		}

		ForkJoinTask<ImmutableList<Integer>, Promise<ImmutableList<Integer>>> fjt = fjp.join(tasks);
		fjt.submit();

		ImmutableList<Integer> l = fjt.compose().await(15, TimeUnit.SECONDS);

		assertThat("Integers were collected", l.size(), is(100));
	}

}
