package reactor.spring.task;

import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import reactor.core.Environment;
import reactor.spring.context.config.EnableReactor;
import reactor.spring.core.task.RingBufferAsyncTaskExecutor;
import reactor.spring.core.task.WorkQueueAsyncTaskExecutor;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jon Brisbin
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TaskExecutorTests.TestConfig.class})
public class TaskExecutorTests {

	static int runs = 1000;

	@Autowired
	WorkQueueAsyncTaskExecutor  workQueue;
	@Autowired
	RingBufferAsyncTaskExecutor ringBuffer;
	AtomicLong counter;
	long       start;
	long       end;
	double     elapsed;
	int        throughput;

	@Before
	public void setup() {
		counter = new AtomicLong(0);
	}

	@Test
	public void testWorkQueueRunnableThroughput() {
		doAsyncRunnableTest("work queue runnable", workQueue);
	}

	@Test
	public void testWorkQueueCallableThroughput() {
		doAsyncCallableTest("work queue callable", workQueue);
	}

	@Test
	public void testRingBufferRunnableThroughput() {
		doAsyncRunnableTest("ring buffer runnable", ringBuffer);
	}

	@Test
	public void testRingBufferCallableThroughput() {
		doAsyncCallableTest("ring buffer callable", ringBuffer);
	}

	private void doStart() {
		start = System.currentTimeMillis();
	}

	private void doStop(String test) {
		end = System.currentTimeMillis();
		elapsed = end - start;
		throughput = (int)(counter.get() / (elapsed / 1000));

		System.out.println(test + " throughput: " + throughput + "/sec");
	}

	private void doAsyncRunnableTest(String test, AsyncTaskExecutor executor) {
		Runnable r = new Runnable() {
			@Override
			public void run() {
				counter.incrementAndGet();
			}
		};

		doStart();
		for(int i = 0; i < runs; i++) {
			executor.execute(r);
		}
		doStop(test);
	}

	@SuppressWarnings("unchecked")
	private void doAsyncCallableTest(String test, AsyncTaskExecutor executor) {
		Callable c = new Callable() {
			@Override
			public Object call() throws Exception {
				return counter.incrementAndGet();
			}
		};

		doStart();
		for(int i = 0; i < runs; i++) {
			executor.submit(c);
		}
		doStop(test);
	}

	@Configuration
	@EnableReactor
	static class TestConfig {

		@Bean
		public WorkQueueAsyncTaskExecutor workQueue(Environment env) {
			return new WorkQueueAsyncTaskExecutor(env)
					.setBacklog(4096)
					.setProducerType(ProducerType.SINGLE)
					.setWaitStrategy(new YieldingWaitStrategy());
		}

		@Bean
		public RingBufferAsyncTaskExecutor ringBuffer(Environment env) {
			return new RingBufferAsyncTaskExecutor(env)
					.setProducerType(ProducerType.SINGLE)
					.setWaitStrategy(new YieldingWaitStrategy());
		}

	}

}
