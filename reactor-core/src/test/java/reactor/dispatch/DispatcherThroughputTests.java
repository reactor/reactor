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

package reactor.dispatch;

import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.Selector;
import reactor.fn.dispatch.RingBufferDispatcher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static reactor.Fn.$;
import static reactor.core.Context.*;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class DispatcherThroughputTests {

	static final int    selectors  = 250;
	static final int    iterations = 7500;
	static final int    testRuns   = 3;

	protected final Logger log        = LoggerFactory.getLogger(getClass());

	Reactor        reactor;
	CountDownLatch latch;

	Consumer<Event<Object>> countDown = new Consumer<Event<Object>>() {
		@Override
		public void accept(Event<Object> ev) {
			latch.countDown();
		}
	};
	Object[] objects = new Object[selectors];
	Selector[]    sels  = new Selector[selectors];
	Event<String> hello = new Event<String>("Hello World!");
	long   start;
	long   end;
	double elapsed;
	long   throughput;

	@Before
	public void start() {
		reactor = new Reactor();
		for (int i = 0; i < selectors; i++) {
			Object object = "test" + i;
			sels[i] = $(object);
			objects[i] = object;
			reactor.on(sels[i], countDown);
		}
		for (int i = 0; i < selectors; i++) {
			// pre-select everything to ensure it's in the cache
			reactor.getConsumerRegistry().select(objects[i]);
		}
	}

	protected void preRun() {
		start = System.currentTimeMillis();
		latch = new CountDownLatch(selectors * iterations);
	}

	protected void postRun() {
		end = System.currentTimeMillis();
		elapsed = (end - start);
		throughput = Math.round((selectors * iterations) / (elapsed / 1000));

		log.info(reactor.getDispatcher().getClass().getSimpleName() + " throughput (" + ((long) elapsed) + "ms): " + throughput + "/sec");
	}

	protected void doTest() throws InterruptedException {
		for (int j = 0; j < testRuns; j++) {
			preRun();
			for (int i = 0; i < selectors * iterations; i++) {
				reactor.notify(objects[i % selectors], hello);
			}
			latch.await(30, TimeUnit.SECONDS);
			postRun();
		}
	}

	@Test
	public void testBlockingQueueDispatcher() throws InterruptedException {
		reactor.setDispatcher(nextWorkerDispatcher());

		log.info("Starting blocking queue test...");
		doTest();
	}

	@Test
	public void testThreadPoolDispatcher() throws InterruptedException {
		reactor.setDispatcher(threadPoolDispatcher());

		log.info("Starting thread pool test...");
		doTest();
	}

	@Test
	public void testRootDispatcher() throws InterruptedException {
		reactor.setDispatcher(rootDispatcher());

		log.info("Starting root RingBuffer test...");
		doTest();
	}

	@Test
	public void testRingBufferDispatcher() throws InterruptedException {
		reactor.setDispatcher(new RingBufferDispatcher("test",
																									 1,
																									 512,
																									 ProducerType.SINGLE,
																									 new YieldingWaitStrategy()));

		log.info("Starting single-producer, yielding RingBuffer test...");
		doTest();
	}

}
