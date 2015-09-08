/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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
package reactor.reactivestreams.tck;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.reactivestreams.tck.TestEnvironment;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import reactor.Processors;
import reactor.Timers;
import reactor.core.support.Assert;
import reactor.rx.Streams;
import reactor.rx.action.Action;
import reactor.rx.broadcast.Broadcaster;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Stephane Maldini
 */
public abstract class AbstractStreamVerification extends org.reactivestreams.tck.IdentityProcessorVerification<Integer> {


	private final Map<Thread, AtomicLong> counters = new ConcurrentHashMap<>();

	protected final int batch = 1024;

	public AbstractStreamVerification() {
		super(new TestEnvironment(500, true));
	}

	final ExecutorService executorService = Executors.newCachedThreadPool();

	final Queue<Processor<Integer, Integer>> processorReferences = new ConcurrentLinkedQueue<>();

	@Override
	public ExecutorService publisherExecutorService() {
		return executorService;
	}

	@BeforeClass
	@Before
	public void setup() {
		Timers.global();
	}

	@AfterClass
	@After
	public void tearDown() {
		executorService.submit(() -> {
			  Processor<Integer, Integer> p;
			  while ((p = processorReferences.poll()) != null) {
				  p.onComplete();
			  }
		  }
		);

		executorService.shutdown();
		Timers.unregisterGlobal();
	}

	@Override
	public Integer createElement(int element) {
		return  element;
	}

	@Override
	public Processor<Integer, Integer> createIdentityProcessor(int bufferSize) {
		final Processor<Integer, Integer> p = createProcessor(bufferSize);

		/*Streams.period(200, TimeUnit.MILLISECONDS)
		  .consume(i -> System.out.println(p.debug()) );*/

		processorReferences.add(p);
		return p;
	}


	@Override
	public Publisher<Integer> createFailedPublisher() {
		return Streams.fail(new Exception("oops")).cast(Integer.class);
	}

	public abstract Processor<Integer, Integer> createProcessor(int bufferSize);

	protected void monitorThreadUse(int val) {
		AtomicLong counter = counters.get(Thread.currentThread());
		if (counter == null) {
			counter = new AtomicLong();
			counters.put(Thread.currentThread(), counter);
		}
		counter.incrementAndGet();
	}

	@Override
	public Publisher<Integer> createHelperPublisher(long elements) {
		if (elements < 100 && elements > 0) {
			List<Integer> list = new ArrayList<Integer>();
			for (int i = 1; i <= elements; i++) {
				list.add(i);
			}

			return Streams
			  .from(list)
			  .log("iterable-publisher")
			  .filter(integer -> true)
			  .map(integer -> integer);

		} else {
			final Random random = new Random();

			return Streams
			  .generate(random::nextInt)
			  .log("random-publisher")
			  .map(Math::abs);
		}
	}


	/*@Test
	public void testAlotOfHotStreams() throws InterruptedException{
		for(int i = 0; i<10000; i++)
			testHotIdentityProcessor();
	}*/

	@Test
	public void testColdIdentityProcessor() throws InterruptedException {
		final int elements = 10;
		CountDownLatch latch = new CountDownLatch(elements + 1);

		Processor<Integer, Integer> processor = createProcessor(16);

		createHelperPublisher(10).subscribe(processor);

		if(Action.class.isAssignableFrom(processor.getClass())) {
			System.out.println(((Action)processor).debug());
		}
		List<Integer> list = new ArrayList<>();

		processor.subscribe(new Subscriber<Integer>() {
			Subscription s;

			@Override
			public void onSubscribe(Subscription s) {
				this.s = s;
				s.request(1);
			}

			@Override
			public void onNext(Integer integer) {
				synchronized (list) {
					list.add(integer);
				}
				latch.countDown();
				if (latch.getCount() > 0) {
					s.request(1);
				}
			}

			@Override
			public void onError(Throwable t) {
				t.printStackTrace();
			}

			@Override
			public void onComplete() {
				System.out.println("completed!");
				latch.countDown();
			}
		});
		//stream.broadcastComplete();

		latch.await(8, TimeUnit.SECONDS);
		if(Action.class.isAssignableFrom(processor.getClass())) {
			System.out.println(((Action)processor).debug());
		}

		System.out.println(counters);
		long count = latch.getCount();
		Assert.state(latch.getCount() == 0, "Count > 0 : " + count + " (" + list + ")  , Running on " + Processors
		  .DEFAULT_POOL_SIZE + " CPU");

	}

	/*@Test
	public void test100Hot() throws InterruptedException {
		for (int i = 0; i < 10000; i++) {
			testHotIdentityProcessor();
		}
	}
*/
	@Test
	public void testHotIdentityProcessor() throws InterruptedException {
		final int elements = 10000;
		CountDownLatch latch = new CountDownLatch(elements);

		Processor<Integer, Integer> processor = createProcessor(1024);

		Broadcaster<Integer> stream = Broadcaster.create();

		stream.subscribe(processor);
		if(Action.class.isAssignableFrom(processor.getClass())) {
			System.out.println(((Action)processor).debug());
		}

		processor.subscribe(new Subscriber<Integer>() {
			@Override
			public void onSubscribe(Subscription s) {
				s.request(elements);
			}

			@Override
			public void onNext(Integer integer) {
				latch.countDown();
			}

			@Override
			public void onError(Throwable t) {
				System.out.println("error! " + t);
			}

			@Override
			public void onComplete() {
				System.out.println("completed!");
				//latch.countDown();
			}
		});


		stream.onNext(0);
		stream.onNext(1);

		System.out.println(stream.debug());
		for (int i = 2; i < elements; i++) {
			stream.onNext(i);
		}
		//stream.broadcastComplete();

		latch.await(8, TimeUnit.SECONDS);
		System.out.println(stream.debug());

		System.out.println(counters);
		long count = latch.getCount();
		Assert.state(latch.getCount() == 0, "Count > 0 : " + count + " , Running on " + Processors.DEFAULT_POOL_SIZE + " " +
		  "CPU");

		stream.onComplete();

	}


	static {
		System.setProperty("reactor.trace.cancel", "true");
	}
}
