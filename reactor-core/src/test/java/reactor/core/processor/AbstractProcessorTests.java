/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.core.processor;

import org.junit.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import reactor.Publishers;
import reactor.Timers;
import reactor.fn.timer.Timer;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 */
public abstract class AbstractProcessorTests extends org.reactivestreams.tck.IdentityProcessorVerification<Long> {

	final ExecutorService executorService = Executors.newCachedThreadPool();

	final Queue<Processor<Long, Long>> processorReferences = new ConcurrentLinkedQueue<>();

	@Test
	public void simpleTest() throws Exception{

	}

	@Override
	public ExecutorService publisherExecutorService() {
		return executorService;
	}

	public AbstractProcessorTests() {
		super(new TestEnvironment(500)., 1000);
	}

	@BeforeClass
	public void setup() {
		Timers.global();
	}

	@AfterClass
	public void tearDown() {
		executorService.submit(() -> {
			  Processor<Long, Long> p;
			  while ((p = processorReferences.poll()) != null) {
				  p.onComplete();
			  }
		  }
		);

		executorService.shutdown();
		Timers.unregisterGlobal();
	}

	@Override
	public Long createElement(int element) {
		return (long) element;
	}

	@Override
	public Processor<Long, Long> createIdentityProcessor(int bufferSize) {
		Processor<Long, Long> p = createProcessor(bufferSize);
		processorReferences.add(p);
		return p;
	}

	protected abstract Processor<Long, Long> createProcessor(int bufferSize);

	/*@Override
	public Publisher<Long> createHelperPublisher(long elements) {
		return Publishers.<Long, AtomicLong>create(
		  (s) -> {
			  long cursor = s.context().getAndIncrement();
			  if (cursor < elements) {
				  s.onNext(cursor);
			  } else {
				  s.onComplete();
			  }
		  },
		  s -> new AtomicLong(0L)
		);
	}*/

	@Override
	public Publisher<Long> createFailedPublisher() {
		return Publishers.error(new Exception("test"));
	}
}
