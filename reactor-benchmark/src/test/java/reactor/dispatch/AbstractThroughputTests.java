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

package reactor.dispatch;

import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.AbstractReactorTest;
import reactor.core.Reactor;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.RingBufferDispatcher;
import reactor.event.selector.Selector;
import reactor.function.Consumer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractThroughputTests extends AbstractReactorTest {

	protected final int                     selectors         = 256;
	protected final int                     testRuns          = 3;
	protected final long                    testDuration      = 5000;
	protected final Object[]                objects           = new Object[selectors];
	protected final Selector[]              sels              = new Selector[selectors];
	protected final Event<String>           hello             = new Event<String>("Hello World!");
	protected final AtomicLong              counter           = new AtomicLong();
	protected final Consumer<Event<Object>> countDownConsumer = new CountDownConsumer();
	protected final Logger                  log               = LoggerFactory.getLogger(getClass());

	private CountDownLatch latch;

	private long start;

	protected void preRun() {
		start = System.currentTimeMillis();
		latch = new CountDownLatch(1);
		counter.set(0);
	}

	protected void postRun(Reactor reactor) throws InterruptedException {
		awaitConsumer();

		long end = System.currentTimeMillis();
		double elapsed = end - start;
		long throughput = Math.round(counter.get() / (elapsed / 1000));

		log.info(reactor.getDispatcher().getClass().getSimpleName() +
				         " throughput (" +
				         ((long)elapsed) +
				         "ms): " +
				         throughput +
				         "/sec");
	}

	private final class CountDownConsumer implements Consumer<Event<Object>> {
		private final Logger log = LoggerFactory.getLogger(getClass());

		@Override
		public void accept(Event<Object> ev) {
			counter.incrementAndGet();
			if(log.isTraceEnabled()) {
				log.trace("Counted down {}", ev);
			}
		}
	}

	protected void awaitConsumer() throws InterruptedException {
		//latch.await(testDuration, TimeUnit.MILLISECONDS);
	}

	protected Dispatcher createRingBufferDispatcher() {
		return new RingBufferDispatcher("test", 512, ProducerType.SINGLE, new YieldingWaitStrategy());
	}
}
