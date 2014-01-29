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

import org.junit.Test;
import reactor.core.Reactor;
import reactor.core.spec.Reactors;
import reactor.event.Event;
import reactor.event.dispatch.EventLoopDispatcher;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;

import java.util.Random;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class DispatcherThroughputTests extends AbstractThroughputTests {

	public void registerConsumersAndWarmCache(Reactor reactor) {
		for(int i = 0; i < selectors; i++) {
			Object object = "test" + i;
			sels[i] = Selectors.$(object);
			objects[i] = object;
			reactor.on(sels[i], countDownConsumer);
		}
		for(int i = 0; i < selectors; i++) {
			// pre-select everything to ensure it's in the cache
			reactor.getConsumerRegistry().select(objects[i]);
		}
	}

	protected void doTest(String dispatcher) throws InterruptedException {
		doTest(Reactors.reactor().env(env).dispatcher(dispatcher).get());
	}

	protected void doTest(Reactor reactor) throws InterruptedException {
		registerConsumersAndWarmCache(reactor);

		for(int j = 0; j < testRuns; j++) {
			preRun();
			long start = System.currentTimeMillis();
			int i = 0;
			do {
				reactor.notify(objects[i++ % selectors], hello);
			} while(System.currentTimeMillis() - start < testDuration);
			postRun(reactor);
		}

		reactor.getDispatcher().shutdown();
	}

	protected void doPrepareTest(String dispatcher) throws InterruptedException {
		doPrepareTest(Reactors.reactor().env(env).dispatcher(dispatcher).get());
	}

	protected void doPrepareTest(Reactor reactor) throws InterruptedException {
		registerConsumersAndWarmCache(reactor);

		final int selectorIdx = new Random().nextInt(selectors);
		Consumer<Event<String>> consumer = reactor.prepare(objects[selectorIdx]);
		for(int j = 0; j < testRuns; j++) {
			preRun();
			long start = System.currentTimeMillis();
			do {
				consumer.accept(hello);
			} while(System.currentTimeMillis() - start < testDuration);
			postRun(reactor);
		}

		reactor.getDispatcher().shutdown();
	}

	@Test
	public void eventLoopDispatcherThroughput() throws InterruptedException {
		log.info("Starting event loop test...");
		doTest(Reactors.reactor()
		               .env(env)
		               .dispatcher(new EventLoopDispatcher("eventLoop", 1024))
		               .get());
	}

	@Test
	public void threadPoolDispatcherThroughput() throws InterruptedException {
		log.info("Starting thread pool test...");
		doTest("threadPoolExecutor");
	}

	@Test
	public void preparedThreadPoolDispatcherThroughput() throws InterruptedException {
		log.info("Starting prepared thread pool test...");
		doPrepareTest("threadPoolExecutor");
	}

	@Test
	public void defaultRingBufferDispatcherThroughput() throws InterruptedException {
		log.info("Starting root RingBuffer test...");
		doTest("ringBuffer");
	}

	@Test
	public void singleProducerRingBufferDispatcherThroughput() throws InterruptedException {
		log.info("Starting single-producer, yielding RingBuffer test...");
		doTest(Reactors.reactor().env(env).dispatcher(createRingBufferDispatcher()).get());
	}

	@Test
	public void preparedRingBufferDispatcherThroughput() throws InterruptedException {
		log.info("Starting prepared RingBuffer test...");
		doPrepareTest("ringBuffer");
	}

	@Test
	public void actorDispatcherThroughput() throws InterruptedException {
		log.info("Starting prepared ActorSystem test...");
		doTest(Reactors.reactor().env(env).actorSystem("eventLoop").get());
	}

}
