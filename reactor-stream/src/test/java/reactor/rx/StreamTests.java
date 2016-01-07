/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
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

package reactor.rx;

import java.awt.event.KeyEvent;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.AbstractReactorTest;
import reactor.Mono;
import reactor.Processors;
import reactor.Subscribers;
import reactor.core.error.CancelException;
import reactor.core.processor.ProcessorGroup;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.publisher.FluxFactory;
import reactor.core.support.Logger;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.rx.broadcast.Broadcaster;
import reactor.rx.broadcast.StreamProcessor;
import reactor.rx.subscriber.Control;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.*;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class StreamTests extends AbstractReactorTest {

	static final String2Integer STRING_2_INTEGER = new String2Integer();

	@Test
	public void testComposeFromSingleValue() throws InterruptedException {
		Stream<String> stream = Streams.just("Hello World!");
		Stream<String> s = stream.map(s1 -> "Goodbye then!");

		await(s, is("Goodbye then!"));
	}

	@Test
	public void testComposeFromMultipleValues() throws InterruptedException {
		Stream<String> stream = Streams.just("1", "2", "3", "4", "5");
		Stream<Integer> s = stream.map(STRING_2_INTEGER)
		                          .map(new Function<Integer, Integer>() {
			                          int sum = 0;

			                          @Override
			                          public Integer apply(Integer i) {
				                          sum += i;
				                          return sum;
			                          }
		                          });
		await(5, s, is(15));
	}

	@Test
	public void simpleReactiveSubscriber() throws InterruptedException {
		Broadcaster<String> str = Broadcaster.create();

		str.dispatchOn(asyncGroup)
		   .subscribe(new TestSubscriber());

		System.out.println(str.debug());

		str.onNext("Goodbye World!");
		str.onNext("Goodbye World!");
		str.onComplete();

		Thread.sleep(500);
	}

	@Test
	public void testComposeFromMultipleFilteredValues() throws InterruptedException {
		Stream<String> stream = Streams.just("1", "2", "3", "4", "5");
		Stream<Integer> s = stream.map(STRING_2_INTEGER)
		                          .filter(i -> i % 2 == 0);

		await(2, s, is(4));
	}

	@Test
	public void testComposedErrorHandlingWithMultipleValues() throws InterruptedException {
		Stream<String> stream = Streams.just("1", "2", "3", "4", "5");

		final AtomicBoolean exception = new AtomicBoolean(false);
		Stream<Integer> s = stream.map(STRING_2_INTEGER)
		                          .map(new Function<Integer, Integer>() {
			                          int sum = 0;

			                          @Override
			                          public Integer apply(Integer i) {
				                          if (i >= 5) {
					                          throw new IllegalArgumentException();
				                          }
				                          sum += i;
				                          return sum;
			                          }
		                          })
		                          .when(IllegalArgumentException.class, e -> exception.set(true));

		await(5, s, is(10));
		assertThat("error triggered", exception.get(), is(true));
	}

	@Test
	public void testReduce() throws InterruptedException {
		Stream<String> stream = Streams.just("1", "2", "3", "4", "5");
		Mono<Integer> s = stream.map(STRING_2_INTEGER)
		                          .reduce(1, (acc, next) -> acc * next);
		await(1, s, is(120));
	}

	@Test
	public void testMerge() throws InterruptedException {
		Stream<String> stream1 = Streams.just("1", "2");
		Stream<String> stream2 = Streams.just("3", "4", "5");
		Mono<Integer> s = Streams.merge(stream1, stream2)
		                           //.dispatchOn(env)
		                           .capacity(5)
		                           .log("merge")
		                           .map(STRING_2_INTEGER)
		                           .reduce(1, (acc, next) -> acc * next);
		await(1, s, is(120));
	}

	@Test
	public void testFirstAndLast() throws InterruptedException {
		Stream<Integer> s = Streams.fromIterable(Arrays.asList(1, 2, 3, 4, 5));

		Stream<Integer> first = s.sampleFirst();
		Stream<Integer> last = s.every(5);

		assertThat("First is 1",
				first.tap()
				     .get(),
				is(1));
		assertThat("Last is 5",
				last.tap()
				    .get(),
				is(5));
	}

	@Test
	public void testStreamBatchesResults() {
		Stream<String> stream = Streams.just("1", "2", "3", "4", "5");
		Stream<List<Integer>> s = stream.map(STRING_2_INTEGER)
		                                .buffer();

		final AtomicInteger batchCount = new AtomicInteger();
		final AtomicInteger count = new AtomicInteger();
		s.consume(is -> {
			batchCount.incrementAndGet();
			for (int i : is) {
				count.addAndGet(i);
			}
		});

		assertThat("batchCount is 3", batchCount.get(), is(1));
		assertThat("count is 15", count.get(), is(15));
	}

	@Test
	public void testHandlersErrorsDownstream() throws InterruptedException {
		Stream<String> stream = Streams.just("1", "2", "a", "4", "5");
		final CountDownLatch latch = new CountDownLatch(1);
		Stream<Integer> s = stream.map(STRING_2_INTEGER)
		                          .map(new Function<Integer, Integer>() {
			                          int sum = 0;

			                          @Override
			                          public Integer apply(Integer i) {
				                          if (i >= 5) {
					                          throw new IllegalArgumentException();
				                          }
				                          sum += i;
				                          return sum;
			                          }
		                          })
		                          .when(NumberFormatException.class, new Consumer<NumberFormatException>() {
			                          @Override
			                          public void accept(NumberFormatException e) {
				                          latch.countDown();
			                          }
		                          });

		await(2, s, is(3));
		assertThat("error handler was invoked", latch.getCount(), is(0L));
	}

	@Test
	public void promiseAcceptCountCannotExceedOne() {
		Promise<Object> deferred = Promise.ready();
		deferred.onNext("alpha");
		try {
			deferred.onNext("bravo");
		}
		catch (CancelException ise) {
			// Swallow
		}
		assertEquals(deferred.peek(), "alpha");
	}

	@Test
	public void promiseErrorCountCannotExceedOne() {
		Promise<Object> deferred = Promise.ready();
		Throwable error = new Exception();
		deferred.onError(error);
		try {
			deferred.onNext(error);
		}
		catch (CancelException ise) {
			// Swallow
		}
		assertTrue(deferred.reason() instanceof Exception);
	}

	@Test
	public void promiseAcceptCountAndErrorCountCannotExceedOneInTotal() {
		Promise<Object> deferred = Promise.ready();
		Throwable error = new Exception();
		deferred.onError(error);
		try {
			deferred.onNext("alpha");
			fail();
		}
		catch (CancelException ise) {
		}
		assertTrue(deferred.reason() instanceof Exception);
	}

	@Test
	public void mapManyFlushesAllValuesThoroughly() throws InterruptedException {
		int items = 1000;
		CountDownLatch latch = new CountDownLatch(items);
		Random random = ThreadLocalRandom.current();

		Broadcaster<String> d = Broadcaster.<String>create();
		Stream<Integer> tasks = d.dispatchOn(asyncGroup)
		                         .partition(8)
		                         .flatMap(stream -> stream.dispatchOn(asyncGroup)
		                                                  .map((String str) -> {
			                                                  try {
				                                                  Thread.sleep(random.nextInt(10));
			                                                  }
			                                                  catch (InterruptedException e) {
				                                                  Thread.currentThread()
				                                                        .interrupt();
			                                                  }
			                                                  return Integer.parseInt(str);
		                                                  }));

		Control tail = tasks.consume(i -> {
			latch.countDown();
		});

		System.out.println(tail.debug());

		for (int i = 1; i <= items; i++) {
			d.onNext(String.valueOf(i));
		}
		latch.await(15, TimeUnit.SECONDS);
		System.out.println(tail.debug());
		assertTrue(latch.getCount() + " of " + items + " items were not counted down", latch.getCount() == 0);
	}

	@Test
	public void mapManyFlushesAllValuesConsistently() throws InterruptedException {
		int iterations = 5;
		for (int i = 0; i < iterations; i++) {
			mapManyFlushesAllValuesThoroughly();
		}
	}

	<T> void await(Stream<T> s, Matcher<T> expected) throws InterruptedException {
		await(1, s, expected);
	}

	<T> void await(int count, final Publisher<T> s, Matcher<T> expected) throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(count);
		final AtomicReference<T> ref = new AtomicReference<T>();
		s.subscribe(Subscribers.consumer(t -> {
			ref.set(t);
			latch.countDown();
		}, t -> {
			t.printStackTrace();
			latch.countDown();
		}));

		long startTime = System.currentTimeMillis();
		T result = null;
		try {
			latch.await(10, TimeUnit.SECONDS);

			result = ref.get();
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		long duration = System.currentTimeMillis() - startTime;

		assertThat(result, expected);
		assertThat(duration, is(lessThan(2000L)));
	}

	static class String2Integer implements Function<String, Integer> {

		@Override
		public Integer apply(String s) {
			return Integer.parseInt(s);
		}
	}

	@Test
	public void mapNotifiesOnceConsistent() throws InterruptedException {
		for (int i = 0; i < 100; i++) {
			mapNotifiesOnce();
		}
	}

	/**
	 * See #294 the consumer received more or less calls than expected Better reproducible with big thread pools, e.g.
	 * 128 threads
	 */
	@Test
	public void mapNotifiesOnce() throws InterruptedException {

		final int COUNT = 10000;
		final Object internalLock = new Object();
		final Object consumerLock = new Object();

		final CountDownLatch internalLatch = new CountDownLatch(COUNT);
		final CountDownLatch counsumerLatch = new CountDownLatch(COUNT);

		final AtomicInteger internalCounter = new AtomicInteger(0);
		final AtomicInteger consumerCounter = new AtomicInteger(0);

		final ConcurrentHashMap<Object, Long> seenInternal = new ConcurrentHashMap<>();
		final ConcurrentHashMap<Object, Long> seenConsumer = new ConcurrentHashMap<>();

		Broadcaster<Integer> d = Broadcaster.create();

		Control c = d.dispatchOn(asyncGroup)
		             .partition(8)
		             .consume(stream -> stream.dispatchOn(asyncGroup)
		                                      .map(o -> {
			                                      synchronized (internalLock) {

				                                      internalCounter.incrementAndGet();

				                                      long curThreadId = Thread.currentThread()
				                                                               .getId();
				                                      Long prevThreadId = seenInternal.put(o, curThreadId);
				                                      if (prevThreadId != null) {
					                                      fail(String.format(
							                                      "The object %d has already been seen internally on the thread %d, current thread %d",
							                                      o,
							                                      prevThreadId,
							                                      curThreadId));
				                                      }

				                                      internalLatch.countDown();
			                                      }
			                                      return -o;
		                                      })
		                                      .consume(o -> {
			                                      synchronized (consumerLock) {
				                                      consumerCounter.incrementAndGet();

				                                      long curThreadId = Thread.currentThread()
				                                                               .getId();
				                                      Long prevThreadId = seenConsumer.put(o, curThreadId);
				                                      if (prevThreadId != null) {
					                                      System.out.println(String.format(
							                                      "The object %d has already been seen by the consumer on the thread %d, current thread %d",
							                                      o,
							                                      prevThreadId,
							                                      curThreadId));
					                                      fail();
				                                      }

				                                      counsumerLatch.countDown();
			                                      }
		                                      }));

		for (int i = 0; i < COUNT; i++) {
			d.onNext(i);
		}

		internalLatch.await(5, TimeUnit.SECONDS);
		System.out.println(c.debug());
		assertEquals(COUNT, internalCounter.get());
		counsumerLatch.await(5, TimeUnit.SECONDS);
		assertEquals(COUNT, consumerCounter.get());
	}

	@Test
	public void analyticsTest() throws Exception {
		Broadcaster<Integer> source = Broadcaster.<Integer>create();
		long avgTime = 50l;

		Promise<Long> result = source.onBackpressureBuffer()
		                          .dispatchOn(asyncGroup)
		                          .throttle(avgTime)
		                          .elapsed()
		                          .skip(1)
		                          .nest()
		                          .flatMap(self -> BiStreams.reduceByKey(self, (acc, next) -> acc + next))
		                          .log("elapsed")
		                          .sort((a, b) -> a.t1.compareTo(b.t1))
		                          .reduce(-1L, (acc, next) -> acc > 0l ? ((next.t1 + acc) / 2) : next.t1)
		                          .log("reduced-elapsed")
		                          .to(Promise.prepare());

		for (int j = 0; j < 10; j++) {
			source.onNext(1);
		}
		source.onComplete();

		Assert.assertTrue(result.await(5, TimeUnit.SECONDS) >= avgTime * 0.6);
	}

	@Test
	public void konamiCode() throws InterruptedException {
		final RingBufferProcessor<Integer> keyboardStream = RingBufferProcessor.create();

		Mono<List<Boolean>> konamis = Streams.from(keyboardStream.start())
		                                     .skipWhile(key -> KeyEvent.VK_UP != key)
		                                     .buffer(10, 1)
		                                     .map(keys -> keys.size() == 10 &&
				                                     keys.get(0) == KeyEvent.VK_UP &&
				                                     keys.get(1) == KeyEvent.VK_UP &&
				                                     keys.get(2) == KeyEvent.VK_DOWN &&
				                                     keys.get(3) == KeyEvent.VK_DOWN &&
				                                     keys.get(4) == KeyEvent.VK_LEFT &&
				                                     keys.get(5) == KeyEvent.VK_RIGHT &&
				                                     keys.get(6) == KeyEvent.VK_LEFT &&
				                                     keys.get(7) == KeyEvent.VK_RIGHT &&
				                                     keys.get(8) == KeyEvent.VK_B &&
				                                     keys.get(9) == KeyEvent.VK_A)
		                                     .toList();

		keyboardStream.onNext(KeyEvent.VK_UP);
		keyboardStream.onNext(KeyEvent.VK_UP);
		keyboardStream.onNext(KeyEvent.VK_UP);
		keyboardStream.onNext(KeyEvent.VK_DOWN);
		keyboardStream.onNext(KeyEvent.VK_DOWN);
		keyboardStream.onNext(KeyEvent.VK_LEFT);
		keyboardStream.onNext(KeyEvent.VK_RIGHT);
		keyboardStream.onNext(KeyEvent.VK_LEFT);
		keyboardStream.onNext(KeyEvent.VK_RIGHT);
		keyboardStream.onNext(KeyEvent.VK_B);
		keyboardStream.onNext(KeyEvent.VK_A);
		keyboardStream.onNext(KeyEvent.VK_C);
		keyboardStream.onComplete();

		List<Boolean> res = konamis.get();

		Assert.assertTrue(res.size() == 12);
		Assert.assertFalse(res.get(0));
		Assert.assertTrue(res.get(1));
		Assert.assertFalse(res.get(2));
		Assert.assertFalse(res.get(3));
		Assert.assertFalse(res.get(4));
		Assert.assertFalse(res.get(5));
		Assert.assertFalse(res.get(6));
		Assert.assertFalse(res.get(7));
		Assert.assertFalse(res.get(8));
		Assert.assertFalse(res.get(9));
		Assert.assertFalse(res.get(10));
		Assert.assertFalse(res.get(11));
	}

	@Test
	public void failureToleranceTest() throws InterruptedException {
		final Broadcaster<String> closeCircuit = Broadcaster.create();
		final Stream<String> openCircuit = Streams.just("Alternative Message");

		final StreamProcessor<Publisher<? extends String>, String> circuitSwitcher = Streams.switchOnNext();

		final AtomicInteger successes = new AtomicInteger();
		final AtomicInteger failures = new AtomicInteger();

		final int maxErrors = 3;

		Promise<List<String>> promise = circuitSwitcher.doOnNext(d -> successes.incrementAndGet())
		                                               .when(Throwable.class, error -> failures.incrementAndGet())
		                                               .doOnSubscribe(s -> {
			                                               if (failures.compareAndSet(maxErrors, 0)) {
				                                               System.out.println("failures: " + failures + " successes:" + successes);
				                                               circuitSwitcher.onNext(openCircuit);
				                                               successes.set(0);
				                                               Streams.timer(1)
				                                                      .consume(ignore -> circuitSwitcher.onNext(
						                                                      closeCircuit));
			                                               }
		                                               })
		                                               .log("faultTolerant")
		                                               .retry()
		                                               .take(6)
		                                               .buffer()
		                                               .promise();

		circuitSwitcher.onNext(closeCircuit);

		closeCircuit.onNext("test1");
		closeCircuit.onNext("test2");
		closeCircuit.onNext("test3");
		closeCircuit.onError(new Exception("test4"));
		closeCircuit.onError(new Exception("test5"));
		closeCircuit.onError(new Exception("test6"));
		Thread.sleep(1500);
		closeCircuit.onNext("test7");
		closeCircuit.onNext("test8");
		closeCircuit.onComplete();
		circuitSwitcher.onComplete();

		List<String> res = promise.await();
		Assert.assertNotNull(res);
		Assert.assertEquals(res.get(0), "test1");
		Assert.assertEquals(res.get(1), "test2");
		Assert.assertEquals(res.get(2), "test3");
		Assert.assertEquals(res.get(3), "Alternative Message");
		Assert.assertEquals(res.get(4), "test7");
		Assert.assertEquals(res.get(5), "test8");
	}

	@Test
	public void parallelTests() throws InterruptedException {
		parallelMapManyTest("sync", 1_000_000);
		parallelMapManyTest("shared", 1_000_000);
		parallelTest("sync", 1_000_000);
		parallelTest("shared", 1_000_000);
		parallelTest("partitioned", 1_000_000);
		parallelMapManyTest("partitioned", 1_000_000);
		parallelBufferedTimeoutTest(1_000_000);
	}

	private void parallelBufferedTimeoutTest(int iterations) throws InterruptedException {

		System.out.println("Buffered Stream: " + iterations);

		final CountDownLatch latch = new CountDownLatch(iterations);

		Broadcaster<String> deferred = Broadcaster.<String>create();
		deferred.dispatchOn(asyncGroup)
		        .partition(8)
		        .consume(stream -> stream.dispatchOn(asyncGroup)
		                                 .buffer(1000 / 8, 1l, TimeUnit.SECONDS)
		                                 .consume(batch -> {
			                                 for (String i : batch) {
				                                 latch.countDown();
			                                 }
		                                 }));

		String[] data = new String[iterations];
		for (int i = 0; i < iterations; i++) {
			data[i] = Integer.toString(i);
		}

		long start = System.currentTimeMillis();

		for (String i : data) {
			deferred.onNext(i);
		}
		if (!latch.await(30, TimeUnit.SECONDS)) {
			throw new RuntimeException(deferred.debug()
			                                   .toString());
		}

		long stop = System.currentTimeMillis() - start;
		stop = stop > 0 ? stop : 1;

		System.out.println("Time spent: " + stop + "ms");
		System.out.println("ev/ms: " + iterations / stop);
		System.out.println("ev/s: " + iterations / stop * 1000);
		System.out.println("");
		System.out.println(deferred.debug());
		assertEquals(0, latch.getCount());
	}

	private void parallelTest(String dispatcher, int iterations) throws InterruptedException {

		System.out.println("Dispatcher: " + dispatcher);
		System.out.println("..........:  " + iterations);

		int[] data;
		CountDownLatch latch = new CountDownLatch(iterations);
		Broadcaster<Integer> deferred;
		switch (dispatcher) {
			case "partitioned":
				deferred = Broadcaster.create();
				System.out.println(deferred.dispatchOn(asyncGroup)
				                           .partition(2)
				                           .consume(stream -> stream.dispatchOn(asyncGroup)
				                                                    .map(i -> i)
				                                                    .scan(1, (acc, next) -> acc + next)
				                                                    .consume(i -> latch.countDown())
				                                                    .debug()));

				break;

			default:
				deferred = Broadcaster.<Integer>create();
				deferred.dispatchOn(asyncGroup)
				        .map(i -> i)
				        .scan(1, (acc, next) -> acc + next)
				        .consume(i -> latch.countDown());
		}

		data = new int[iterations];
		for (int i = 0; i < iterations; i++) {
			data[i] = i;
		}

		long start = System.currentTimeMillis();
		for (int i : data) {
			deferred.onNext(i);
		}

		if (!latch.await(15, TimeUnit.SECONDS)) {
			throw new RuntimeException("Count:" + (iterations - latch.getCount()) + " " + deferred.debug()
			                                                                                      .toString());
		}

		long stop = System.currentTimeMillis() - start;
		stop = stop > 0 ? stop : 1;

		System.out.println("Time spent: " + stop + "ms");
		System.out.println("ev/ms: " + iterations / stop);
		System.out.println("ev/s: " + iterations / stop * 1000);
		System.out.println("");
		System.out.println(deferred.debug());
		assertEquals(0, latch.getCount());

	}

	private void parallelMapManyTest(String dispatcher, int iterations) throws InterruptedException {

		System.out.println("MM Dispatcher: " + dispatcher);
		System.out.println("..........:  " + iterations);

		int[] data;
		CountDownLatch latch = new CountDownLatch(iterations);
		Broadcaster<Integer> mapManydeferred;
		switch (dispatcher) {
			case "partitioned":
				mapManydeferred = Broadcaster.<Integer>create();
				mapManydeferred.partition(4)
				               .consume(substream -> substream.dispatchOn(asyncGroup)
				                                              .consume(i -> latch.countDown()));
				break;
			default:
				mapManydeferred = Broadcaster.<Integer>create();
				mapManydeferred.dispatchOn("sync".equals(dispatcher) ? ProcessorGroup.sync() : asyncGroup)
				               .flatMap(Streams::just)
				               .consume(i -> latch.countDown());
		}

		data = new int[iterations];
		for (int i = 0; i < iterations; i++) {
			data[i] = i;
		}

		long start = System.currentTimeMillis();

		for (int i : data) {
			mapManydeferred.onNext(i);
		}

		if (!latch.await(20, TimeUnit.SECONDS)) {
			throw new RuntimeException(mapManydeferred.debug()
			                                          .toString());
		}
		else {
			System.out.println(mapManydeferred.debug()
			                                  .toString());
		}
		assertEquals(0, latch.getCount());

		long stop = System.currentTimeMillis() - start;
		stop = stop > 0 ? stop : 1;

		System.out.println("MM Dispatcher: " + dispatcher);
		System.out.println("Time spent: " + stop + "ms");
		System.out.println("ev/ms: " + iterations / stop);
		System.out.println("ev/s: " + iterations / stop * 1000);
		System.out.println("");
	}

	/**
	 * See https://github.com/reactor/reactor/issues/451
	 */
	@Test
	public void partitionByHashCodeShouldNeverCreateMoreStreamsThanSpecified() throws Exception {
		Stream<Integer> stream = Streams.range(-10, 20)
		                                .map(Integer::intValue);

		assertThat(stream.partition(2)
		                 .count()
		                 .get(), is(equalTo(2L)));
	}

	/**
	 * original from @oiavorskyl https://github.com/eventBus/eventBus/issues/358
	 */
	//@Test
	public void shouldNotFlushStreamOnTimeoutPrematurelyAndShouldDoItConsistently() throws Exception {
		for (int i = 0; i < 100; i++) {
			shouldNotFlushStreamOnTimeoutPrematurely();
		}
	}

	/**
	 * original from @oiavorskyl https://github.com/eventBus/eventBus/issues/358
	 */
	@Test
	public void shouldNotFlushStreamOnTimeoutPrematurely() throws Exception {
		final int NUM_MESSAGES = 100000;
		final int BATCH_SIZE = 1000;
		final int TIMEOUT = 100;
		final int PARALLEL_STREAMS = 2;

		/**
		 * Relative tolerance, default to 90% of the batches, in an operative environment, random factors can impact
		 * the stream latency, e.g. GC pause if system is under pressure.
		 */
		final double TOLERANCE = 0.9;

		Broadcaster<Integer> batchingStreamDef = Broadcaster.from(asyncGroup);

		List<Integer> testDataset = createTestDataset(NUM_MESSAGES);

		final CountDownLatch latch = new CountDownLatch(NUM_MESSAGES);
		Map<Integer, Integer> batchesDistribution = new ConcurrentHashMap<>();
		batchingStreamDef.partition(PARALLEL_STREAMS)
		                 .consume(substream -> substream.dispatchOn(asyncGroup)
		                                                .buffer(BATCH_SIZE, TIMEOUT, TimeUnit.MILLISECONDS)
		                                                .consume(items -> {
			                                                batchesDistribution.compute(items.size(),
					                                                (key, value) -> value == null ? 1 : value + 1);
			                                                items.forEach(item -> latch.countDown());
		                                                }));

		testDataset.forEach(batchingStreamDef::onNext);

		System.out.println(batchingStreamDef.debug());

		System.out.println(batchesDistribution);

		if (!latch.await(10, TimeUnit.SECONDS)) {
			throw new RuntimeException(latch.getCount() + " " + batchingStreamDef.debug()
			                                                                     .toString());

		}

		int messagesProcessed = batchesDistribution.entrySet()
		                                           .stream()
		                                           .mapToInt(entry -> entry.getKey() * entry.getValue())
		                                           .reduce(Integer::sum)
		                                           .getAsInt();

		assertEquals(NUM_MESSAGES, messagesProcessed);
		assertTrue("Less than 90% (" + NUM_MESSAGES / BATCH_SIZE * TOLERANCE +
						") of the batches are matching the buffer() size: " + batchesDistribution.get(BATCH_SIZE),
				NUM_MESSAGES / BATCH_SIZE * TOLERANCE >= batchesDistribution.get(BATCH_SIZE) * TOLERANCE);
	}

	@Test
	public void prematureFlatMapCompletion() throws Exception {

		long res = Streams.range(0, 1_000_000)
		       .flatMap(v -> Streams.range(v, 2))
		       .count()
		       .get(5, TimeUnit.SECONDS);

		assertTrue("Latch is " + res, res == 2_000_000);
	}

	@Test
	public void zipOfNull() {
		try {
			Stream<String> as = Streams.just("x");
			Stream<String> bs = Streams.<String>just(null);

			assertNull(Streams.zip(as, bs) .next().get());
		}
		catch (NullPointerException npe) {
			return;
		}
		assertFalse("Should have failed", true);

	}

	@Test
	public void shouldCorrectlyDispatchComplexFlow() throws InterruptedException {
		Broadcaster<Integer> globalFeed = Broadcaster.create();

		CountDownLatch afterSubscribe = new CountDownLatch(1);
		CountDownLatch latch = new CountDownLatch(4);

		Stream<Integer> s = Streams.just("2222")
		                           .map(Integer::parseInt)
		                           .flatMap(l -> Streams.<Integer>merge(globalFeed.dispatchOn(asyncGroup),
				                           Streams.just(1111, l, 3333, 4444, 5555, 6666)).log("merged")
		                                                                                 .dispatchOn(asyncGroup)
		                                                                                 .log("dispatched")
		                                                                                 .doOnSubscribe(x -> afterSubscribe.countDown())
		                                                                                 .filter(nearbyLoc -> 3333 >= nearbyLoc)
		                                                                                 .filter(nearbyLoc -> 2222 <= nearbyLoc)

		                           );

		Control action = s.capacity(1L)
		                  .consume(integer -> {
			                  latch.countDown();
			                  System.out.println(integer);
		                  });

		System.out.println(action.debug());

		afterSubscribe.await(5, TimeUnit.SECONDS);

		System.out.println(action.debug());

		globalFeed.onNext(2223);
		globalFeed.onNext(2224);

		latch.await(5, TimeUnit.SECONDS);
		System.out.println(action.debug());
		assertEquals("Must have counted 4 elements", 0, latch.getCount());

	}

	@Test
	public void testParallelAsyncStream2() throws InterruptedException {

		final int numOps = 25;

		CountDownLatch latch = new CountDownLatch(numOps);

		for (int i = 0; i < numOps; i++) {
			final String source = "ASYNC_TEST " + i;

			Streams.just(source)
			       .liftProcessor(() -> Processors.blackbox(Broadcaster.<String>create(),
					       operationStream -> operationStream.dispatchOn(asyncGroup)
					                                         .throttle(100)
					                                         .map(s -> s + " MODIFIED")
					                                         .map(s -> {
						                                         latch.countDown();
						                                         return s;
					                                         })))
			       .take(2, TimeUnit.SECONDS)
			       .log("parallelStream")
			       .consume(System.out::println);
		}

		latch.await(15, TimeUnit.SECONDS);
		assertEquals(0, latch.getCount());
	}

	/**
	 * https://gist.github.com/nithril/444d8373ce67f0a8b853 Contribution by Nicolas Labrot
	 */
	@Test
	public void testParallelWithJava8StreamsInput() throws InterruptedException {
		ProcessorGroup<Long> supplier = Processors.asyncGroup("test-p", 2048, 2);

		int max = ThreadLocalRandom.current()
		                           .nextInt(100, 300);
		CountDownLatch countDownLatch = new CountDownLatch(max);

		Stream<Integer> worker = Streams.range(0, max)
		                                .dispatchOn(asyncGroup);
		worker.partition(2)
		      .consume(s -> s.dispatchOn(supplier)
		                     .map(v -> v)
		                     .consume(v -> countDownLatch.countDown()));

		countDownLatch.await(10, TimeUnit.SECONDS);
		Assert.assertEquals(0, countDownLatch.getCount());
	}

	@Test
	public void testBeyondLongMaxMicroBatching() throws InterruptedException {
		List<Integer> tasks = IntStream.range(0, 1500)
		                               .boxed()
		                               .collect(Collectors.toList());

		CountDownLatch countDownLatch = new CountDownLatch(tasks.size());
		Stream<Integer> worker = Streams.fromIterable(tasks)
		                                .dispatchOn(asyncGroup);

		Control tail = worker.partition(2)
		                     .consume(s -> s.dispatchOn(asyncGroup)
		                                    .map(v -> v)
		                                    .consume(v -> countDownLatch.countDown(), Throwable::printStackTrace));

		countDownLatch.await(5, TimeUnit.SECONDS);
		if (countDownLatch.getCount() > 0) {
			System.out.println(tail.debug());
		}
		Assert.assertEquals(0, countDownLatch.getCount());
	}

	@Test
	@Ignore
	public void testDiamond() throws InterruptedException, IOException {
		ExecutorService pool = Executors.newCachedThreadPool(new NamedDaemonThreadFactory("tee", null, null, true));

		Stream<Point> points = Streams.from(FluxFactory.<Double, Random>create(sub -> sub.onNext(sub.context()
		                                                                                            .nextDouble()),
				sub -> new Random()))
		                              .log("points")
		                              //.requestWhen(requests -> requests.dispatchOn(Environment.cachedDispatcher()))
		                              .buffer(2)
		                              .map(pairs -> new Point(pairs.get(0), pairs.get(1)))
		                              .process(RingBufferProcessor.create(pool,
				                              32)); //.broadcast(); works because no async boundary

		Stream<InnerSample> innerSamples = points.log("inner-1")
		                                         .filter(Point::isInner)
		                                         .map(InnerSample::new)
		                                         .log("inner-2");

		Stream<OuterSample> outerSamples = points.log("outer-1")
		                                         .filter(p -> !p.isInner())
		                                         .map(OuterSample::new)
		                                         .log("outer-2");

		Streams.merge(innerSamples, outerSamples)
		       .dispatchOn(asyncGroup)
		       .scan(new SimulationState(0l, 0l), SimulationState::withNextSample)
		       .log("result")
		       .map(s -> System.out.printf("After %8d samples π is approximated as %.5f", s.totalSamples, s.pi()))
		       .take(10000)
		       .consume();

		System.in.read();
	}

	private static final class Point {

		final Double x, y;

		public Point(Double x, Double y) {
			this.x = x;
			this.y = y;
		}

		boolean isInner() {
			return x * x + y * y < 1.0;
		}

		@Override
		public String toString() {
			return "Point{" +
					"x=" + x +
					", y=" + y +
					'}';
		}
	}

	private static class Sample {

		final Point point;

		public Sample(Point point) {
			this.point = point;
		}

		@Override
		public String toString() {
			return "Sample{" +
					"point=" + point +
					'}';
		}
	}

	private static final class InnerSample extends Sample {

		public InnerSample(Point point) {
			super(point);
		}
	}

	private static final class OuterSample extends Sample {

		public OuterSample(Point point) {
			super(point);
		}
	}

	private static final class SimulationState {

		final Long totalSamples;
		final Long inCircle;

		public SimulationState(Long totalSamples, Long inCircle) {
			this.totalSamples = totalSamples;
			this.inCircle = inCircle;
		}

		Double pi() {
			return (inCircle.doubleValue() / totalSamples) * 4.0;
		}

		SimulationState withNextSample(Sample sample) {
			return new SimulationState(totalSamples + 1, sample instanceof InnerSample ? inCircle + 1 : inCircle);
		}

		@Override
		public String toString() {
			return "SimulationState{" +
					"totalSamples=" + totalSamples +
					", inCircle=" + inCircle +
					'}';
		}
	}

	@Test
	public void shouldWindowCorrectly() throws InterruptedException {
		Stream<Integer> sensorDataStream = Streams.fromIterable(createTestDataset(1000));

		CountDownLatch endLatch = new CountDownLatch(1000 / 100);

		Control controls = sensorDataStream
				/*     step 2  */.window(100)
				///*     step 3  */.timeout(1000)
				/*     step 4  */
				.consume(batchedStream -> {
					System.out.println("New window starting");
					batchedStream
						/*   step 4.1  */.reduce(Integer.MAX_VALUE, Math::min)
						/* ad-hoc step */
						.doOnSuccess(v -> endLatch.countDown())
						/* final step  */
						.subscribe(Subscribers.consumer(i -> System.out.println("Minimum " + i)));
				});

		endLatch.await(10, TimeUnit.SECONDS);
		System.out.println(controls.debug());

		Assert.assertEquals(0, endLatch.getCount());
	}

	@Test
	public void shouldThrottleCorrectly() throws InterruptedException {
		Streams.range(1, 10000000)
		       .dispatchOn(asyncGroup)
		       .requestWhen(reqs -> reqs.flatMap(req -> {
			       // set the batch size
			       long batchSize = 10;

			       // Value below in reality should be req / batchSize;
			       // Now test for 30, 60, 500, 1000, 1000000 etc and see what happens
			       // small nubmers should work, but I think there is bug
			       long numBatches = 1000;

			       System.out.println("Original request = " + req);
			       System.out.println("Batch size = " + batchSize);
			       System.out.println("Number of batches should be = " + numBatches);

			       // LongRangeStream for correct handling of values > 4 byte int
			       // return new LongRangeStream(1, numBatches).map(x->batchSize);
			       return Streams.range(1, (int) numBatches)
			                     .map(x -> batchSize);
		       }))
		       .consume();
	}

	@Test
	public void shouldCorrectlyDispatchBatchedTimeout() throws InterruptedException {

		long timeout = 100;
		final int batchsize = 4;
		int parallelStreams = 16;
		CountDownLatch latch = new CountDownLatch(1);

		final Broadcaster<Integer> streamBatcher = Broadcaster.<Integer>create();
		streamBatcher.dispatchOn(asyncGroup)
		             .buffer(batchsize, timeout, TimeUnit.MILLISECONDS)
		             .log("batched")
		             .partition(parallelStreams)
		             .log("batched-inner")
		             .consume(innerStream -> innerStream.dispatchOn(asyncGroup)
		                                                .when(Exception.class, Throwable::printStackTrace)
		                                                .consume(i -> latch.countDown()));

		streamBatcher.onNext(12);
		streamBatcher.onNext(123);
		streamBatcher.onNext(42);
		streamBatcher.onNext(666);

		boolean finished = latch.await(2, TimeUnit.SECONDS);
		if (!finished) {
			throw new RuntimeException(streamBatcher.debug()
			                                        .toString());
		}
		else {
			System.out.println(streamBatcher.debug()
			                                .toString());
			assertEquals("Must have correct latch number : " + latch.getCount(), latch.getCount(), 0);
		}
	}

	@Test
	public void mapLotsOfSubAndCancel() throws InterruptedException {
		for (long i = 0; i < 199; i++) {
			mapPassThru();
		}
	}

	public void mapPassThru() throws InterruptedException {
		Streams.just(1)
		       .map(IDENTITY_FUNCTION);
	}

	@Test
	public void consistentMultithreadingWithPartition() throws InterruptedException {
		ProcessorGroup<Long> supplier1 = Processors.asyncGroup("groupByPool", 32, 2);
		ProcessorGroup<Long> supplier2 = Processors.asyncGroup("partitionPool", 32, 5);

		CountDownLatch latch = new CountDownLatch(10);

		Control c = Streams.range(1, 10)
		                   .groupBy(n -> n % 2 == 0)
		                   .flatMap(stream -> stream.dispatchOn(supplier1)
		                                            .log("groupBy-" + stream.key()))
		                   .partition(5)
		                   .flatMap(stream -> stream.dispatchOn(supplier2)
		                                            .log("partition-" + stream.key()))
		                   .dispatchOn(asyncGroup)
		                   .log("join")
		                   .consume(t -> {
			                   latch.countDown();
		                   });

		System.out.println(c.debug());

		latch.await(30, TimeUnit.SECONDS);
		assertThat("Not totally dispatched: " + latch.getCount(), latch.getCount() == 0);
		supplier1.shutdown();
		supplier2.shutdown();
	}

	@Test
	public void subscribeOnDispatchOn() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(100);

		Streams.range(1, 100)
		       .log("testOn")
		       .publishOn(ioGroup)
		       .dispatchOn(asyncGroup)
		       .capacity(1)
		       .consume(t -> latch.countDown());

		assertThat("Not totally dispatched", latch.await(30, TimeUnit.SECONDS));
	}

	// Test issue https://github.com/reactor/reactor/issues/474
// code by @masterav10
	@Test
	public void combineWithOneElement() throws InterruptedException, TimeoutException {
		AtomicReference<Object> ref = new AtomicReference<>(null);

		Phaser phaser = new Phaser(2);

		Stream<Object> s1 = Broadcaster.replayLastOrDefault(new Object())
		                               .dispatchOn(asyncGroup);
		Stream<Object> s2 = Broadcaster.replayLastOrDefault(new Object())
		                               .dispatchOn(asyncGroup);

		// The following works:
		//List<Stream<Object>> list = Arrays.asList(s1);
		// The following fails:
		List<Stream<Object>> list = Arrays.asList(s1, s2);

		Streams.combineLatest(list, t -> t)
		       .log()
		       .doOnNext(obj -> {
			       ref.set(obj);
			       phaser.arrive();
		       })
		       .consume();

		phaser.awaitAdvanceInterruptibly(phaser.arrive(), 1, TimeUnit.SECONDS);
		Assert.assertNotNull(ref.get());
	}

	/**
	 * This test case demonstrates a silent failure of {@link Streams#period(long)} when a resolution is specified that
	 * is less than the backing {@link Timer} class.
	 *
	 * @throws InterruptedException - on failure.
	 * @throws TimeoutException     - on failure. <p> by @masterav10 : https://github.com/reactor/reactor/issues/469
	 */
	@Test
	@Ignore
	public void endLessTimer() throws InterruptedException, TimeoutException {
		int tasks = 50;
		long delayMS = 50; // XXX: Fails when less than 100
		Phaser barrier = new Phaser(tasks + 1);

		List<Long> times = new ArrayList<>();

		// long localTime = System.currentTimeMillis(); for java 7
		long localTime = Instant.now()
		                        .toEpochMilli();
		long elapsed = System.nanoTime();

		Control ctrl = Streams.period(delayMS, TimeUnit.MILLISECONDS)
		                      .map((signal) -> {
			                      return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - elapsed);
		                      })
		                      .doOnNext((elapsedMillis) -> {
			                      times.add(localTime + elapsedMillis);
			                      barrier.arrive();
		                      })
		                      .consume();

		barrier.awaitAdvanceInterruptibly(barrier.arrive(), tasks * delayMS + 1000, TimeUnit.MILLISECONDS);
		ctrl.cancel();

		Assert.assertEquals(tasks, times.size());

		for (int i = 1; i < times.size(); i++) {
			Long prev = times.get(i - 1);
			Long time = times.get(i);

			Assert.assertTrue(prev > 0);
			Assert.assertTrue(time > 0);
			Assert.assertTrue("was " + (time - prev), time - prev <= delayMS * 1.2);
		}
	}

	private static final Function<Integer, Integer> IDENTITY_FUNCTION = new Function<Integer, Integer>() {
		@Override
		public Integer apply(Integer value) {
			return value;
		}
	};

	private List<Integer> createTestDataset(int i) {
		List<Integer> list = new ArrayList<>(i);
		for (int k = 0; k < i; k++) {
			list.add(k);
		}
		return list;
	}

	class TestSubscriber implements Subscriber<String> {

		private final Logger log = Logger.getLogger(getClass());

		private Subscription subscription;

		@Override
		public void onSubscribe(Subscription subscription) {
			if (null != this.subscription) {
				subscription.cancel();
				return;
			}
			this.subscription = subscription;
			this.subscription.request(1);
		}

		@Override
		public void onNext(String s) {
			if (s.startsWith("GOODBYE")) {
				log.info("This is the end");
			}
			subscription.request(1);
		}

		@Override
		public void onError(Throwable throwable) {
			log.error(throwable.getMessage(), throwable);
		}

		@Override
		public void onComplete() {
			log.info("stream complete");
		}

	}

	/**
	 * Should work with {@link Processor} but it doesn't.
	 */
	//@Test
	public void forkJoinUsingProcessors1000() throws Exception {
		for (int i = 0; i < 1000; i++) {
			System.out.println("new test " + i);
			forkJoinUsingProcessors();
			System.out.println();
		}
	}

	@Test(timeout = TIMEOUT)
	public void forkJoinUsingProcessors() throws Exception {

		final Stream<Integer> forkStream = Streams.just(1, 2, 3)
		                                          .log("begin-computation");
		final Stream<Integer> forkStream2 = Streams.just(1, 2, 3)
		                                           .log("begin-persistence");

		final RingBufferProcessor<Integer> computationBroadcaster = RingBufferProcessor.create("computation", BACKLOG);
		final Stream<String> computationStream = Streams.from(computationBroadcaster)
		                                                .map(i -> Integer.toString(i));

		final RingBufferProcessor<Integer> persistenceBroadcaster = RingBufferProcessor.create("persistence", BACKLOG);
		final Stream<String> persistenceStream = Streams.from(persistenceBroadcaster)
		                                                .map(i -> "done " + i);

		forkStream.subscribe(computationBroadcaster);
		forkStream2.subscribe(persistenceBroadcaster);

		final Semaphore doneSemaphore = new Semaphore(0);

		final Stream<List<String>> joinStream =
				Streams.join(computationStream.log("log1"), persistenceStream.log("log2"));

		// Method chaining doesn't compile.
		joinStream.log("log-final")
		          .consume(list -> println("Joined: ", list), t -> println("Join failed: ", t.getMessage()), () -> {
			          println("Join complete.");
			          doneSemaphore.release();
		          });

		doneSemaphore.acquire();

	}

	/**
	 * <pre>
	 *                 forkStream
	 *                 /        \      < - - - int
	 *                v          v
	 * persistenceStream        computationStream
	 *                 \        /      < - - - List< String >
	 *                  v      v
	 *                 joinStream      < - - - String
	 *                 splitStream
	 *             observedSplitStream
	 * </pre>
	 */
	@Test(timeout = TIMEOUT)
	public void forkJoinUsingDispatchersAndSplit() throws Exception {

		final Broadcaster<Integer> forkBroadcaster = Broadcaster.create();

		final Broadcaster<Integer> computationBroadcaster = Broadcaster.create();
		final Stream<List<String>> computationStream =
				computationBroadcaster.dispatchOn(Processors.singleGroup("computation", BACKLOG))
				                      .map(i -> {
					                      final List<String> list = new ArrayList<>(i);
					                      for (int j = 0; j < i; j++) {
						                      list.add("i" + j);
					                      }
					                      return list;
				                      })
				                      .doOnNext(ls -> println("Computed: ", ls))
				                      .log("computation");

		final Broadcaster<Integer> persistenceBroadcaster = Broadcaster.create();
		final Stream<List<String>> persistenceStream =
				persistenceBroadcaster.dispatchOn(Processors.singleGroup("persistence", BACKLOG))
				                      .doOnNext(i -> println("Persisted: ", i))
				                      .map(i -> Collections.singletonList("done" + i))
				                      .log("persistence");

		Stream<Integer> forkStream = forkBroadcaster.dispatchOn(Processors.singleGroup("fork", BACKLOG))
		                                            .log("fork");

		forkStream.subscribe(computationBroadcaster);
		forkStream.subscribe(persistenceBroadcaster);

		final Stream<List<String>> joinStream = Streams.join(computationStream, persistenceStream)
		                                               .dispatchOn(Processors.singleGroup("join", BACKLOG))
		                                               .map(listOfLists -> {
			                                               listOfLists.get(0)
			                                                          .addAll(listOfLists.get(1));
			                                               return listOfLists.get(0);
		                                               })
		                                               .log("join");

		final Semaphore doneSemaphore = new Semaphore(0);

		final Mono<List<String>> listPromise = joinStream.flatMap(Streams::fromIterable)
		                                                 .log("resultStream")
		                                                 .toList()
		                                                 .doOnTerminate((v, e) -> doneSemaphore.release());

		System.out.println(forkBroadcaster.debug());

		forkBroadcaster.onNext(1);
		forkBroadcaster.onNext(2);
		forkBroadcaster.onNext(3);
		forkBroadcaster.onComplete();

		List<String> res = listPromise.get(5, TimeUnit.SECONDS);
		System.out.println(forkBroadcaster.debug());
		assertEquals(Arrays.asList("i0", "done1", "i0", "i1", "done2", "i0", "i1", "i2", "done3"), res);
	}

	@Test
	@Ignore
	public void splitBugEventuallyHappens() throws Exception {
		int successCount = 0;
		try {
			for (; ; ) {
				forkJoinUsingDispatchersAndSplit();
				println("**** Success! ****");
				successCount++;
			}
		}
		finally {
			println("Succeeded " + successCount + " time" + (successCount <= 1 ? "." : "s."));
		}

	}

	private static final long TIMEOUT = 10_000;

	// Setting it to 1 doesn't help.
	private static final int BACKLOG = 1024;

	private static void println(final Object... fragments) {
		final Thread currentThread = Thread.currentThread();
		synchronized (System.out) {
			System.out.print(String.format("[%s] ", currentThread.getName()));
			for (final Object fragment : fragments) {
				System.out.print(fragment);
			}
			System.out.println();
		}
	}

}
