/*
 * Copyright (c) 2011-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core;

import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.Before;
import org.junit.Test;
import reactor.AbstractReactorTest;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.support.Reduce;
import reactor.fn.dispatch.Dispatcher;
import reactor.fn.dispatch.RingBufferDispatcher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Jon Brisbin
 */
public class ComposableThroughputTests extends AbstractReactorTest {

	static int length  = 500;
	static int runs    = 1000;
	static int samples = 3;

	List<Integer>  data;
	CountDownLatch latch;

	@Before
	public void setup() {
		data = new ArrayList<Integer>();
		for (int i = 0; i < length; i++) {
			data.add(i);
		}

		latch = new CountDownLatch(length * runs * samples);
	}

	private Composable<Integer> createComposable(Dispatcher dispatcher) {
		Composable<Integer> cInt = R.<Integer>defer().using(env).using(dispatcher).get();
		cInt.map(new Function<Integer, Integer>() {
			@Override
			public Integer apply(Integer integer) {
				return integer;
			}
		})
				.reduce(new Function<Reduce<Integer, Integer>, Integer>() {
					@Override
					public Integer apply(Reduce<Integer, Integer> r) {
						int last = (null != r.getLastValue() ? r.getLastValue() : 1);
						return last + r.getNextValue();
					}
				})
				.consume(new Consumer<Integer>() {
					@Override
					public void accept(Integer integer) {
						latch.countDown();
					}
				});
		return cInt;
	}

	private void doTest(Dispatcher dispatcher, String name) throws InterruptedException {
		Composable<Integer> c = createComposable(dispatcher);
		long start = System.currentTimeMillis();
		for (int x = 0; x < samples; x++) {
			for (int i = 0; i < runs; i++) {
				for (int j = 0; j < length; j++) {
					c.accept(j);
				}
			}
		}

		latch.await(1, TimeUnit.SECONDS);

		long end = System.currentTimeMillis();
		long elapsed = end - start;

		System.out.println(String.format("%s throughput (%sms): %s",
																		 name,
																		 elapsed,
																		 Math.round((length * runs * samples) / (elapsed * 1.0 / 1000)) + "/sec"));

		dispatcher.shutdown();
	}

	@Test
	public void testThreadPoolDispatcherComposableThroughput() throws InterruptedException {
		doTest(env.getDispatcher(Environment.THREAD_POOL_EXECUTOR_DISPATCHER), "thread pool");
	}

	@Test
	public void testEventLoopDispatcherComposableThroughput() throws InterruptedException {
		doTest(env.getDispatcher(Environment.EVENT_LOOP_DISPATCHER), "event loop");
	}

	@Test
	public void testRingBufferDispatcherComposableThroughput() throws InterruptedException {
		doTest(env.getDispatcher(Environment.RING_BUFFER_DISPATCHER), "ring buffer");
	}

	@Test
	public void testSingleProducerRingBufferDispatcherComposableThroughput() throws InterruptedException {
		doTest(new RingBufferDispatcher("test", 1, 1024, ProducerType.SINGLE, new YieldingWaitStrategy()), "single-producer ring buffer");
	}

}
