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

import org.junit.Before;
import reactor.core.Reactor;
import reactor.fn.Registry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static reactor.Fn.$;

/**
 * @author Jon Brisbin
 */
public class LoadBalancingThroughputTests extends DispatcherThroughputTests {

	@Override
	@Before
	public void start() {
		reactor = new Reactor();
		for (int i = 0; i < selectors; i++) {
			int j = i % 10;
			objects[i] = "test" + j;
			sels[i] = $(objects[i]);
			reactor.on(sels[i], countDown);
		}
		for (int i = 0; i < selectors; i++) {
			// pre-select everything to ensure it's in the cache
			reactor.getConsumerRegistry().select(objects[i]);
		}
		latch = new CountDownLatch(selectors * iterations);
	}

	@Override
	protected void doTest() throws InterruptedException {
		for (int j = 0; j < testRuns; j++) {
			preRun();
			for (int i = 0; i < selectors * iterations; i++) {
				int x = (i % selectors) % 10;
				reactor.notify(objects[x], hello);
			}
			latch.await(30, TimeUnit.SECONDS);
			postRun();
		}
	}

	@Override
	public void testBlockingQueueDispatcher() throws InterruptedException {
		reactor.getConsumerRegistry().setLoadBalancingStrategy(Registry.LoadBalancingStrategy.RANDOM);
		super.testBlockingQueueDispatcher();
	}

	@Override
	public void testThreadPoolDispatcher() throws InterruptedException {
		reactor.getConsumerRegistry().setLoadBalancingStrategy(Registry.LoadBalancingStrategy.RANDOM);
		super.testThreadPoolDispatcher();
	}

	@Override
	public void testRootDispatcher() throws InterruptedException {
		reactor.getConsumerRegistry().setLoadBalancingStrategy(Registry.LoadBalancingStrategy.RANDOM);
		super.testRingBufferDispatcher();
	}

	@Override
	public void testRingBufferDispatcher() throws InterruptedException {
		reactor.getConsumerRegistry().setLoadBalancingStrategy(Registry.LoadBalancingStrategy.RANDOM);
		super.testRingBufferDispatcher();
	}

}
