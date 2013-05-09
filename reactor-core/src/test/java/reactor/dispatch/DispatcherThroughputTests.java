/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.
 *
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package reactor.dispatch;

import com.lmax.disruptor.BusySpinWaitStrategy;
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

	static final Logger LOG        = LoggerFactory.getLogger(DispatcherThroughputTests.class);
	static final int    selectors  = 500;
	static final int    iterations = 2000;
	static final int    testRuns   = 3;

	Reactor        reactor;
	CountDownLatch latch;
	@SuppressWarnings({"rawtypes"})
	Consumer<Event<Object>> countDown = new Consumer<Event<Object>>() {
		@Override
		public void accept(Event<Object> ev) {
			latch.countDown();
		}
	};
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
			sels[i] = $("test" + i);
			reactor.on(sels[i], countDown);
		}
		for (int i = 0; i < selectors; i++) {
			// pre-select everything to ensure it's in the cache
			reactor.getConsumerRegistry().select(sels[i]);
		}
		latch = new CountDownLatch(selectors * iterations);
	}

	protected void startTimer() {
		start = System.currentTimeMillis();
	}

	protected void stopTimer() {
		end = System.currentTimeMillis();
		elapsed = (end - start);
		throughput = Math.round((selectors * iterations) / (elapsed / 1000));

		LOG.info(reactor.getDispatcher().getClass().getSimpleName() + " throughput (" + ((long) elapsed) + "ms): " + throughput + "/sec");
	}

	protected void doTest() throws InterruptedException {
		for (int j = 0; j < testRuns; j++) {
			startTimer();
			for (int i = 0; i < selectors * iterations; i++) {
				reactor.notify(sels[i % selectors], hello);
			}
			latch.await(30, TimeUnit.SECONDS);
			stopTimer();
		}
	}

	@Test
	public void testBlockingQueueDispatcher() throws InterruptedException {
		reactor.setDispatcher(nextWorkerDispatcher());

		LOG.info("Starting blocking queue test...");
		doTest();
	}

	@Test
	public void testWorkerPoolDispatcher() throws InterruptedException {
		reactor.setDispatcher(workerPoolDispatcher());

		LOG.info("Starting worker pool test...");
		doTest();
	}

	@Test
	public void testRootDispatcher() throws InterruptedException {
		reactor.setDispatcher(rootDispatcher());

		LOG.info("Starting root (RingBuffer) test...");
		doTest();
	}

	@Test
	public void testRingBufferDispatcher() throws InterruptedException {
		reactor.setDispatcher(new RingBufferDispatcher("test",
																									 1,
																									 1024,
																									 ProducerType.SINGLE,
																									 new BusySpinWaitStrategy()));

		LOG.info("Starting single, busy spin RingBuffer test...");
		doTest();
	}

}
