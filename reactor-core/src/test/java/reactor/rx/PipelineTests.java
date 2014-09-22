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

package reactor.rx;

import org.hamcrest.Matcher;
import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.AbstractReactorTest;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.Reactors;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.event.dispatch.SynchronousDispatcher;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.support.Tap;
import reactor.jarjar.com.lmax.disruptor.BlockingWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;
import reactor.rx.action.ParallelAction;
import reactor.rx.spec.Promises;
import reactor.rx.spec.Streams;
import reactor.tuple.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.*;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class PipelineTests extends AbstractReactorTest {

	static final String2Integer STRING_2_INTEGER = new String2Integer();

	@Test
	public void testComposeFromSingleValue() throws InterruptedException {
		Stream<String> stream = Streams.defer("Hello World!");
		Stream<String> s =
				stream
						.map(s1 -> "Goodbye then!");

		await(s, is("Goodbye then!"));
	}

	@Test
	public void testComposeFromMultipleValues() throws InterruptedException {
		Stream<String> stream = Streams.defer("1", "2", "3", "4", "5");
		Stream<Integer> s =
				stream
						.map(STRING_2_INTEGER)
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
	public void testComposeFromMultipleFilteredValues() throws InterruptedException {
		Stream<String> stream = Streams.defer("1", "2", "3", "4", "5");
		Stream<Integer> s =
				stream
						.map(STRING_2_INTEGER)
						.filter(i -> i % 2 == 0);

		await(2, s, is(4));
	}

	@Test
	public void testComposedErrorHandlingWithMultipleValues() throws InterruptedException {
		Stream<String> stream = Streams.defer(env, "1", "2", "3", "4", "5");

		final AtomicBoolean exception = new AtomicBoolean(false);
		Stream<Integer> s =
				stream
						.map(STRING_2_INTEGER)
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
		assertThat("exception triggered", exception.get(), is(true));
	}


	@Test
	public void testComposedErrorHandlingWitIgnoreErrors() throws InterruptedException {
		Stream<String> stream = Streams.defer(env, "1", "2", "3", "4", "5");

		final AtomicBoolean exception = new AtomicBoolean(false);
		Stream<Integer> s =
				stream
						.map(STRING_2_INTEGER)
						.observe(i -> {
							if (i == 3)
								throw new IllegalArgumentException();
						})
						.ignoreErrors(true)
						.map(new Function<Integer, Integer>() {
							int sum = 0;

							@Override
							public Integer apply(Integer i) {
								sum += i;
								return sum;
							}
						})
						.when(IllegalArgumentException.class, e -> exception.set(true));

		await(4, s, is(12));
		assertThat("exception triggered", exception.get(), is(false));
	}

	@Test
	public void testReduce() throws InterruptedException {
		Stream<String> stream = Streams.defer("1", "2", "3", "4", "5");
		Stream<Integer> s =
				stream
						.map(STRING_2_INTEGER)
						.reduce(r -> r.getT1() * r.getT2(), 1);
		await(1, s, is(120));
	}

	@Test
	public void testMerge() throws InterruptedException {
		Stream<String> stream1 = Streams.defer("1", "2");
		Stream<String> stream2 = Streams.defer("3", "4", "5");
		Stream<Integer> s =
				Streams.merge(env, stream1, stream2)
						.capacity(5)
						.map(STRING_2_INTEGER)
						.reduce(r -> r.getT1() * r.getT2(), 1);
		await(1, s, is(120));
	}

	@Test
	public void testFirstAndLast() throws InterruptedException {
		Stream<Integer> s = Streams.defer(Arrays.asList(1, 2, 3, 4, 5));

		Stream<Integer> first = s.first();
		Stream<Integer> last = s.last();

		assertThat("First is 1", first.tap().get(), is(1));
		assertThat("Last is 5", last.tap().get(), is(5));
	}

	@Test
	public void testRelaysEventsToReactor() throws InterruptedException {
		Reactor r = Reactors.reactor().get();
		Selector key = Selectors.$();

		final CountDownLatch latch = new CountDownLatch(5);
		final Tap<Event<Integer>> tap = new Tap<Event<Integer>>() {
			@Override
			public void accept(Event<Integer> integerEvent) {
				super.accept(integerEvent);
				latch.countDown();
			}
		};

		r.on(key, tap);

		Stream<String> stream = Streams.defer("1", "2", "3", "4", "5");
		Stream<Void> s =
				stream
						.map(STRING_2_INTEGER)
						.notify(key.getObject(), r);
		System.out.println(s.debug());

		//await(s, is(5));
		assertThat("latch was counted down", latch.getCount(), is(0l));
		assertThat("value is 5", tap.get().getData(), is(5));
	}

	@Test
	public void testStreamBatchesResults() {
		Stream<String> stream = Streams.defer("1", "2", "3", "4", "5");
		Stream<List<Integer>> s =
				stream
						.map(STRING_2_INTEGER)
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
		Stream<String> stream = Streams.defer("1", "2", "a", "4", "5");
		final CountDownLatch latch = new CountDownLatch(1);
		Stream<Integer> s =
				stream
						.map(STRING_2_INTEGER)
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
		Promise<Object> deferred = Promises.<Object>defer();
		deferred.onNext("alpha");
		try {
			deferred.onNext("bravo");
		} catch (IllegalStateException ise) {
			// Swallow
		}
		assertEquals(deferred.get(), "alpha");
	}

	@Test
	public void promiseErrorCountCannotExceedOne() {
		Promise<Object> deferred = Promises.defer();
		Throwable error = new Exception();
		deferred.onError(error);
		try {
			deferred.onNext(error);
		} catch (IllegalStateException ise) {
			// Swallow
		}
		assertTrue(deferred.reason() instanceof Exception);
	}

	@Test
	public void promiseAcceptCountAndErrorCountCannotExceedOneInTotal() {
		Promise<Object> deferred = Promises.defer();
		Throwable error = new Exception();
		deferred.onError(error);
		try {
			deferred.onNext("alpha");
			fail();
		} catch (IllegalStateException ise) {
		}
		assertTrue(deferred.reason() instanceof Exception);
	}

	@Test
	public void mapManyFlushesAllValuesThoroughly() throws InterruptedException {
		int items = 1000;
		CountDownLatch latch = new CountDownLatch(items);
		Random random = ThreadLocalRandom.current();

		Stream<String> d = Streams.<String>defer(env).capacity(128);
		Stream<Integer> tasks = d.parallel(8)
				.map(stream -> stream.map(str -> {
							try {
								Thread.sleep(random.nextInt(10));
							} catch (InterruptedException e) {
								Thread.currentThread().interrupt();
							}
							return Integer.parseInt(str);
						})
				).merge();

		tasks.consume(i -> {
			latch.countDown();
		});

		System.out.println(tasks.debug());

		for (int i = 1; i <= items; i++) {
			d.broadcastNext(String.valueOf(i));
		}
		latch.await(15, TimeUnit.SECONDS);
		System.out.println(tasks.debug());
		assertTrue(latch.getCount() + " of " + items + " items were counted down", latch.getCount() == 0);
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

	<T> void await(int count, final Stream<T> s, Matcher<T> expected) throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(count);
		final AtomicReference<T> ref = new AtomicReference<T>();
		s.when(Exception.class, e -> {
			e.printStackTrace();
			latch.countDown();
		}).consume(t -> {
			System.out.println(s.debug());
			ref.set(t);
			latch.countDown();
		});

		System.out.println(s.debug());

		long startTime = System.currentTimeMillis();
		T result = null;
		try {
			latch.await(10, TimeUnit.SECONDS);
			result = ref.get();
		} catch (Exception e) {
			e.printStackTrace();
		}
		long duration = System.currentTimeMillis() - startTime;

		System.out.println(s.debug());
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
		for (int i = 0; i < 15; i++) {
			mapNotifiesOnce();
		}
	}

	/**
	 * See #294 the consumer received more or less calls than expected Better reproducible with big thread pools,
	 * e.g. 128
	 * threads
	 *
	 * @throws InterruptedException
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

		Stream<Integer> d = Streams.defer(env);

		d.parallel().consume(stream -> stream.map(o -> {
			synchronized (internalLock) {

				internalCounter.incrementAndGet();

				long curThreadId = Thread.currentThread().getId();
				Long prevThreadId = seenInternal.put(o, curThreadId);
				if (prevThreadId != null) {
					fail(String.format(
							"The object %d has already been seen internally on the thread %d, current thread %d",
							o, prevThreadId, curThreadId));
				}

				internalLatch.countDown();
			}
			return -o;
		}).consume(o -> {
			synchronized (consumerLock) {
				consumerCounter.incrementAndGet();

				long curThreadId = Thread.currentThread().getId();
				Long prevThreadId = seenConsumer.put(o, curThreadId);
				if (prevThreadId != null) {
					System.out.println(String.format(
							"The object %d has already been seen by the consumer on the thread %d, current thread %d",
							o, prevThreadId, curThreadId));
					fail();
				}

				counsumerLatch.countDown();
			}
		}));

		for (int i = 0; i < COUNT; i++) {
			d.broadcastNext(i);
		}


		System.out.println(d.debug());
		internalLatch.await(5, TimeUnit.SECONDS);
		System.out.println(d.debug());
		assertEquals(COUNT, internalCounter.get());
		assertTrue(counsumerLatch.await(5, TimeUnit.SECONDS));
		assertEquals(COUNT, consumerCounter.get());
	}


	@Test
	public void parallelTests() throws InterruptedException {
		parallelTest("sync", 1_000_000);
		parallelMapManyTest("sync", 1_000_000);
		parallelTest("ringBuffer", 1_000_000);
		parallelMapManyTest("ringBuffer", 100_000);
		parallelTest("partitioned", 1_000_000);
		parallelMapManyTest("partitioned", 1_000_000);
		parallelBufferedTimeoutTest(1_000_000, false);
	}

	private void parallelBufferedTimeoutTest(int iterations, final boolean filter) throws InterruptedException {


		System.out.println("Buffered Stream: " + iterations);

		final CountDownLatch latch = new CountDownLatch(iterations);

		Stream<String> deferred = Streams.<String>defer(env);
		deferred
				.parallel(8)
				.monitorLatency(100)
				.consume(stream -> (filter ? (stream
								.filter(i -> i.hashCode() != 0 ? true : true)) : stream)
								.buffer(1000 / 8)
								.timeout(1000)
								.consume(batch -> {
									for (String i : batch) latch.countDown();
								})
				);

		String[] data = new String[iterations];
		for (int i = 0; i < iterations; i++) {
			data[i] = Integer.toString(i);
		}

		long start = System.currentTimeMillis();

		for (String i : data) {
			deferred.broadcastNext(i);
		}
		if (!latch.await(30, TimeUnit.SECONDS))
			throw new RuntimeException(deferred.debug().toString());


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
		Stream<Integer> deferred;
		switch (dispatcher) {
			case "partitioned":
				ParallelAction<Integer> parallelStream = Streams.<Integer>parallel(env);
				parallelStream
						.consume(stream -> stream
										.map(i -> i)
										.scan((Tuple2<Integer, Integer> tup) -> {
											int last = (null != tup.getT2() ? tup.getT2() : 1);
											return last + tup.getT1();
										})
										.consume(i -> latch.countDown())
						);

				deferred = Streams.defer(env);
				deferred.connect(parallelStream);
				break;

			default:
				deferred = Streams.<Integer>defer(env, env.getDispatcher(dispatcher));
				deferred
						.map(i -> i)
						.scan((Tuple2<Integer, Integer> tup) -> {
							int last = (null != tup.getT2() ? tup.getT2() : 1);
							return last + tup.getT1();
						})
						.consume(i -> latch.countDown());
		}

		data = new int[iterations];
		for (int i = 0; i < iterations; i++) {
			data[i] = i;
		}

		long start = System.currentTimeMillis();
		for (int i : data) {
			deferred.broadcastNext(i);
		}

		if (!latch.await(15, TimeUnit.SECONDS)) {
			throw new RuntimeException("Count:"+(iterations - latch.getCount())+" "+deferred.debug().toString());
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
		Stream<Integer> mapManydeferred;
		switch (dispatcher) {
			case "partitioned":
				mapManydeferred = Streams.<Integer>defer(env);
				mapManydeferred
						.parallel()
						.consume(substream ->
										substream.consume(i -> latch.countDown())
						);
				break;
			default:
				Dispatcher dispatcher1 = env.getDispatcher(dispatcher);
				mapManydeferred = Streams.<Integer>defer(env, dispatcher1);
				mapManydeferred
						.flatMap(i -> Streams.defer(env, dispatcher1, i))
						.consume(i -> latch.countDown());
		}

		data = new int[iterations];
		for (int i = 0; i < iterations; i++) {
			data[i] = i;
		}

		long start = System.currentTimeMillis();

		for (int i : data) {
			mapManydeferred.broadcastNext(i);
		}

		if (!latch.await(20, TimeUnit.SECONDS)) {
			throw new RuntimeException(mapManydeferred.debug().toString());
		}
		assertEquals(0, latch.getCount());


		long stop = System.currentTimeMillis() - start;
		stop = stop > 0 ? stop : 1;

		System.out.println("Dispatcher: " + dispatcher);
		System.out.println("Time spent: " + stop + "ms");
		System.out.println("ev/ms: " + iterations / stop);
		System.out.println("ev/s: " + iterations / stop * 1000);
		System.out.println("");
	}


	/**
	 * original from @oiavorskyl
	 * https://github.com/reactor/reactor/issues/358
	 *
	 * @throws Exception
	 */
	//@Test
	public void shouldNotFlushStreamOnTimeoutPrematurelyAndShouldDoItConsistently() throws Exception {
		for (int i = 0; i < 100; i++) {
			shouldNotFlushStreamOnTimeoutPrematurely();
		}
	}

	/**
	 * original from @oiavorskyl
	 * https://github.com/reactor/reactor/issues/358
	 *
	 * @throws Exception
	 */
	@Test
	public void shouldNotFlushStreamOnTimeoutPrematurely() throws Exception {
		final int NUM_MESSAGES = 1000000;
		final int BATCH_SIZE = 1000;
		final int TIMEOUT = 100;
		final int PARALLEL_STREAMS = 2;

		/**
		 * Relative tolerance, default to 90% of the batches, in an operative environment, random factors can impact
		 * the stream latency, e.g. GC pause if system is under pressure.
		 */
		final double TOLERANCE = 0.9;


		Stream<Integer> batchingStreamDef = Streams.defer(env);

		List<Integer> testDataset = createTestDataset(NUM_MESSAGES);

		final CountDownLatch latch = new CountDownLatch(NUM_MESSAGES);
		Map<Integer, Integer> batchesDistribution = new ConcurrentHashMap<>();
		batchingStreamDef.parallel(PARALLEL_STREAMS)
				.consume(substream ->
								substream
										.buffer(BATCH_SIZE)
										.timeout(TIMEOUT)
										.consume(items -> {
											batchesDistribution.compute(items.size(),
													(key,
													 value) -> value == null ? 1 : value + 1);
											items.forEach(item -> latch.countDown());
										})

				);

		testDataset.forEach(batchingStreamDef::broadcastNext);
		if (!latch.await(30, TimeUnit.SECONDS)) {
			throw new RuntimeException(batchingStreamDef.debug().toString());

		}

		int messagesProcessed = batchesDistribution.entrySet()
				.stream()
				.mapToInt(entry -> entry.getKey() * entry
						.getValue())
				.reduce(Integer::sum).getAsInt();

		System.out.println(batchingStreamDef.debug());

		assertEquals(NUM_MESSAGES, messagesProcessed);
		System.out.println(batchesDistribution);
		assertTrue("Less than 90% (" + NUM_MESSAGES / BATCH_SIZE * TOLERANCE +
						") of the batches are matching the buffer() size: " + batchesDistribution.get(BATCH_SIZE),
				NUM_MESSAGES / BATCH_SIZE * TOLERANCE >= batchesDistribution.get(BATCH_SIZE) * TOLERANCE);
	}


	@Test
	public void shouldCorrectlyDispatchComplexFlow() throws InterruptedException {
		Stream<Integer> globalFeed = Streams.defer(env);

		CountDownLatch afterSubscribe = new CountDownLatch(1);
		CountDownLatch latch = new CountDownLatch(4);

		Stream<Integer> s = Streams.defer("2222")
				.map(Integer::parseInt)
				.flatMap(l ->
								Streams.merge(
										env,
										globalFeed,
										Streams.defer(1111, l, 3333, 4444, 5555, 6666)
								)
										.observe(x -> afterSubscribe.countDown())
										.filter(nearbyLoc -> 3333 >= nearbyLoc)
										.filter(nearbyLoc -> 2222 <= nearbyLoc)
				);

		s.subscribe(new Subscriber<Integer>() {
			Subscription s;

			@Override
			public void onSubscribe(Subscription s) {
				this.s = s;
				s.request(1);
			}

			@Override
			public void onNext(Integer integer) {
				latch.countDown();
				System.out.println(integer);
				s.request(1);
			}

			@Override
			public void onError(Throwable t) {
				t.printStackTrace();
			}

			@Override
			public void onComplete() {

			}
		});

		afterSubscribe.await(5, TimeUnit.SECONDS);

		globalFeed.broadcastNext(2223);
		globalFeed.broadcastNext(2224);

		latch.await(5, TimeUnit.SECONDS);
		System.out.println(s.debug());
		assertEquals("Must have counted 4 elements", 0, latch.getCount());

	}


	/**
	 * https://gist.github.com/nithril/444d8373ce67f0a8b853
	 * Contribution by Nicolas Labrot
	 */
	@Test
	public void testParallelWithJava8StreamsInput() throws InterruptedException {
		env.addDispatcherFactory("test-p",
				Environment.createDispatcherFactory("test-p", 5, 2048, null, ProducerType.MULTI, new BlockingWaitStrategy()));

		//System.out.println("Java "+ ManagementFactory.getRuntimeMXBean().getVmVersion());
		List<Integer> tasks =
				IntStream.range(0, ThreadLocalRandom.current().nextInt(100,300))
				.boxed()
						.collect(Collectors.toList());

		CountDownLatch countDownLatch = new CountDownLatch(tasks.size());

		Stream<Integer> worker = Streams.defer(env, env.getDispatcherFactory("test-p").get(), tasks);

		worker.parallel(4, env.getDispatcherFactory("test-p")).consume(s -> s.map(v -> v).consume(v -> countDownLatch.countDown()));
		countDownLatch.await(5, TimeUnit.SECONDS);
		System.out.println(worker.debug());
		Assert.assertEquals(0, countDownLatch.getCount());
	}

	@Test
	public void testBeyondLongMaxMicroBatching() throws InterruptedException {
		List<Integer> tasks = IntStream.range(0, 1500).boxed().collect(Collectors.toList());

		CountDownLatch countDownLatch = new CountDownLatch(tasks.size());

		Stream<Integer> worker = Streams.defer(env, env.getDefaultDispatcherFactory().get(), tasks);

		worker.parallel(4).consume(s -> s.map(v -> v).consume(v -> countDownLatch.countDown()));
		countDownLatch.await(5, TimeUnit.SECONDS);

		Assert.assertEquals(0, countDownLatch.getCount());
	}

	@Test
	public void shouldWindowCorrectly() throws InterruptedException{
		Stream<Integer> sensorDataStream = Streams.defer(env, SynchronousDispatcher.INSTANCE, createTestDataset(1000));
		CountDownLatch endLatch = new CountDownLatch(1000/100);

		sensorDataStream
				/*     step 2  */.window(100)
				///*     step 3  */.timeout(1000)
				/*     step 4  */.consume(batchedStream -> {
			System.out.println("New window starting");
			batchedStream
						/*   step 4.1  */.reduce(tuple -> Math.min(tuple.getT1(), tuple.getT2()), Integer.MAX_VALUE)
						/* final step  */.consume(i -> System.out.println("Minimum " + i))
						/* ad-hoc step */.finallyDo(o -> endLatch.countDown());
		});

		endLatch.await(10, TimeUnit.SECONDS);

		Assert.assertEquals(0, endLatch.getCount());
	}

	@Test
	public void shouldCorrectlyDispatchBatchedTimeout() throws InterruptedException {

		long timeout = 100;
		final int batchsize = 4;
		int parallelStreams = 16;
		CountDownLatch latch = new CountDownLatch(1);

		final Stream<Integer> streamBatcher = Streams.<Integer>defer(env);
		streamBatcher
				.buffer(batchsize)
				.timeout(timeout)
				.parallel(parallelStreams)
				.consume(innerStream ->
								innerStream
										.observe(System.out::println)
										.consume(i -> latch.countDown())
										.when(Exception.class, Throwable::printStackTrace)
				);


		streamBatcher.broadcastNext(12);
		streamBatcher.broadcastNext(123);
		streamBatcher.broadcastNext(42);
		streamBatcher.broadcastNext(666);

		boolean finished = latch.await(2, TimeUnit.SECONDS);
		if (!finished)
			throw new RuntimeException(streamBatcher.debug().toString());
		else {
			System.out.println(streamBatcher.debug().toString());
			assertEquals("Must have correct latch number : " + latch.getCount(), latch.getCount(), 0);
		}
	}

	private List<Integer> createTestDataset(int i) {
		List<Integer> list = new ArrayList<>(i);
		for (int k = 0; k < i; k++) {
			list.add(k);
		}
		return list;
	}


}
