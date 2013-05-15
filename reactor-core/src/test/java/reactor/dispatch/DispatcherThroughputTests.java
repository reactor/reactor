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

import static reactor.Fn.$;
import static reactor.core.Context.nextWorkerDispatcher;
import static reactor.core.Context.rootDispatcher;
import static reactor.core.Context.threadPoolDispatcher;

import org.junit.Test;

import reactor.core.Reactor;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class DispatcherThroughputTests extends AbstractThroughputTests {

	public void registerConsumersAndWarmCache(Reactor reactor) {
		for (int i = 0; i < selectors; i++) {
			Object object = "test" + i;
			sels[i] = $(object);
			objects[i] = object;
			reactor.on(sels[i], countDownConsumer);
		}
		for (int i = 0; i < selectors; i++) {
			// pre-select everything to ensure it's in the cache
			reactor.getConsumerRegistry().select(objects[i]);
		}
	}

	protected void doTest(Reactor reactor) throws InterruptedException {
		registerConsumersAndWarmCache(reactor);

		for (int j = 0; j < testRuns; j++) {
			preRun();
			for (int i = 0; i < selectors * iterations; i++) {
				reactor.notify(objects[i % selectors], hello);
			}
			postRun(reactor);
		}
	}

	@Test
	public void blockingQueueDispatcherThroughput() throws InterruptedException {
		log.info("Starting blocking queue test...");
		doTest(new Reactor(nextWorkerDispatcher()));
	}

	@Test
	public void threadPoolDispatcherThroughput() throws InterruptedException {
		log.info("Starting thread pool test...");
		doTest(new Reactor(threadPoolDispatcher()));
	}

	@Test
	public void rootDispatcherThroughput() throws InterruptedException {
		log.info("Starting root RingBuffer test...");
		doTest(new Reactor(rootDispatcher()));
	}

	@Test
	public void ringBufferDispatcherThroughput() throws InterruptedException {
		log.info("Starting single-producer, yielding RingBuffer test...");
		doTest(new Reactor(createRingBufferDispatcher()));
	}

}
