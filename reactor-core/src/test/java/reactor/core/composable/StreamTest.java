package reactor.core.composable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.AbstractReactorTest;
import reactor.core.Environment;
import reactor.core.composable.spec.Streams;
import reactor.function.Consumer;

public class StreamTest extends AbstractReactorTest {
	static final Logger log = LoggerFactory.getLogger(StreamTest.class);

	@Test
	public void testThredPoolStreamConsumer() throws InterruptedException {
		final int ITEMS_SIZE = 100;
		final CountDownLatch latch1 = new CountDownLatch(ITEMS_SIZE);

		final int results1[] = new int[ITEMS_SIZE];
		
		Deferred<Integer, Stream<Integer>> head = Streams.<Integer>defer()
			.env(env)
			.dispatcher(Environment.THREAD_POOL)
			.get();
		
		Stream<Integer> tail1 = head.compose();
		
		tail1.consume(new Consumer<Integer>() {
			@Override
			public void accept(Integer item) {
				assertTrue( Thread.currentThread().getName() + " unexpected " + item, 
						results1[item.intValue()] <= 0 );
				log.debug(Thread.currentThread().getName() + " accepted "+item);
				results1[item.intValue()]++;
				latch1.countDown();
			}
		});
		
		for(int i=0; i<ITEMS_SIZE; i++) {
			head.accept(i);
		}
		
		assertTrue(latch1.await(5, TimeUnit.SECONDS));
		
		for(int i=0; i<ITEMS_SIZE; i++) {
			assertEquals(1, results1[i]);
		}
	}
	
	@Test
	public void testThreadPoolBatching() throws InterruptedException {
		final int ITEMS_SIZE = 12;
		final int BATCH_SIZE = 3;
		final int EXPECTED_ITEMS = ITEMS_SIZE / BATCH_SIZE * BATCH_SIZE;
		final CountDownLatch latch = new CountDownLatch(EXPECTED_ITEMS);

		final int results[] = new int[ITEMS_SIZE];
		
		Deferred<Integer, Stream<Integer>> head = Streams.<Integer>defer()
			.env(env)
			.dispatcher(Environment.THREAD_POOL)
			.batchSize(BATCH_SIZE)
			.get();
		
		Stream<List<Integer>> tail = head.compose().collect();

		tail.consume(new Consumer<List<Integer>>() {
			@Override
			public void accept(List<Integer> batch) {
				log.debug(Thread.currentThread().getName() + " accepted BATCH "+batch);
				for(Integer i : batch) {
					assertTrue( Thread.currentThread().getName() + " unexpected " + i, 
							results[i.intValue()] <= 0 );
					log.debug(Thread.currentThread().getName() + " accepted BATCH item " + i);
					results[i.intValue()]++;
					latch.countDown();
				}
			}
		});
		
		for(int i=0; i<ITEMS_SIZE; i++) {
			head.accept(i);
		}
		
		assertTrue(latch.await(5, TimeUnit.SECONDS));
		
		for(int i=0; i<ITEMS_SIZE; i++) {
			assertEquals(1, results[i]);
		}
	}

}
