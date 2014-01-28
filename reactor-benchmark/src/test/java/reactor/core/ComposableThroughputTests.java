/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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
import org.junit.Test;
import reactor.AbstractReactorTest;
import reactor.core.composable.Composable;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.core.composable.spec.Promises;
import reactor.core.composable.spec.Streams;
import reactor.event.dispatch.ActorDispatcher;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.EventLoopDispatcher;
import reactor.event.dispatch.RingBufferDispatcher;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.tuple.Tuple2;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ComposableThroughputTests extends AbstractReactorTest {

	static int length  = 500;
	static int runs    = 1000;
	static int samples = 3;

	CountDownLatch latch;

	private Deferred<Integer, Stream<Integer>> createDeferred(Dispatcher dispatcher) {
		latch = new CountDownLatch(1);
		Deferred<Integer, Stream<Integer>> dInt = Streams.<Integer>defer()
		                                                 .env(env)
		                                                 .dispatcher(dispatcher)
		                                                 .batchSize(length * runs * samples)
		                                                 .get();
		dInt.compose()
		    .map(new Function<Integer, Integer>() {
			    @Override
			    public Integer apply(Integer integer) {
				    return integer;
			    }
		    })
		    .reduce(new Function<Tuple2<Integer, Integer>, Integer>() {
			    @Override
			    public Integer apply(Tuple2<Integer, Integer> r) {
				    int last = (null != r.getT2() ? r.getT2() : 1);
				    return last + r.getT1();
			    }
		    })
		    .consume(new Consumer<Integer>() {
			    @Override
			    public void accept(Integer integer) {
				    latch.countDown();
			    }
		    });
		return dInt;
	}

	private Deferred<Integer, Stream<Integer>> createMapManyDeferred() {
		latch = new CountDownLatch(length * runs * samples);
		final Dispatcher dispatcher = env.getDefaultDispatcher();
		final Deferred<Integer, Stream<Integer>> dInt = Streams.defer(env, dispatcher);
		dInt.compose()
		    .mapMany(new Function<Integer, Composable<Integer>>() {
			    @Override
			    public Composable<Integer> apply(Integer integer) {
				    Deferred<Integer, Promise<Integer>> deferred = Promises.defer(env, dispatcher);
				    try {
					    return deferred.compose();
				    } finally {
					    deferred.accept(integer);
				    }
			    }
		    })
		    .consume(new Consumer<Integer>() {
			    @Override
			    public void accept(Integer integer) {
				    latch.countDown();
			    }
		    });
		return dInt;
	}

	private void doTestMapMany(String name) throws InterruptedException {
		doTest(env.getDefaultDispatcher(), name, createMapManyDeferred());
	}

	private void doTest(Dispatcher dispatcher, String name) throws InterruptedException {
		doTest(dispatcher, name, createDeferred(dispatcher));
	}

	private void doTest(Dispatcher dispatcher,
	                    String name,
	                    Deferred<Integer, Stream<Integer>> d) throws InterruptedException {
		long start = System.currentTimeMillis();
		for(int x = 0; x < samples; x++) {
			for(int i = 0; i < runs; i++) {
				for(int j = 0; j < length; j++) {
					d.accept(j);
				}
			}
		}

		latch.await(10, TimeUnit.SECONDS);
		assertEquals("Missing accepted events, possibly due to a backlog/batch issue", 0, latch.getCount());

		long end = System.currentTimeMillis();
		long elapsed = end - start;

		System.out.println(String.format("%s throughput (%sms): %s",
		                                 name,
		                                 elapsed,
		                                 Math.round((length * runs * samples) / (elapsed * 1.0 / 1000)) + "/sec"));

		if(dispatcher != null) {
			dispatcher.shutdown();
		}
	}

	@Test
	public void testThreadPoolDispatcherComposableThroughput() throws InterruptedException {
		doTest(env.getDispatcher("threadPoolExecutor"), "thread pool");
	}

	@Test
	public void testEventLoopDispatcherComposableThroughput() throws InterruptedException {
		doTest(new EventLoopDispatcher("eventLoop", 256), "event loop");
	}

	@Test
	public void testRingBufferDispatcherComposableThroughput() throws InterruptedException {
		doTest(env.getDispatcher("ringBuffer"), "ring buffer");
	}

	@Test
	public void testActorDispatcherComposableThroughput() throws InterruptedException {
		doTest(new ActorDispatcher(new Function<Object, Dispatcher>() {
			@Override
			public Dispatcher apply(Object o) {
				return env.getDispatcher("eventLoop");
			}
		}), "actor system");
	}

	@Test
	public void testSingleProducerRingBufferDispatcherComposableThroughput() throws InterruptedException {
		doTest(new RingBufferDispatcher(
				"test",
				1024,
				ProducerType.SINGLE,
				new YieldingWaitStrategy()
		), "single-producer ring buffer");
	}

	@Test
	public void testRingBufferDispatcherMapManyComposableThroughput() throws InterruptedException {
		doTestMapMany("single-producer ring buffer map many");
	}

}
