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

package reactor.core.composable;

import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import org.hamcrest.Matcher;
import org.junit.Test;
import reactor.AbstractReactorTest;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.spec.Promises;
import reactor.core.composable.spec.Streams;
import reactor.core.spec.Reactors;
import reactor.event.Event;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Predicate;
import reactor.function.support.Tap;
import reactor.tuple.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.*;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ComposableTests extends AbstractReactorTest {

	static final String2Integer STRING_2_INTEGER = new String2Integer();

	@Test
	public void testComposeFromSingleValue() throws InterruptedException {
		Stream<String> stream = Streams.defer("Hello World!").get();
		Stream<String> s =
				stream
						.map(new Function<String, String>() {
							@Override
							public String apply(String s) {
								return "Goodbye then!";
							}
						});

		await(s, is("Goodbye then!"));
	}

	@Test
	public void testComposeFromMultipleValues() throws InterruptedException {
		Stream<String> stream = Streams.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
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
		Stream<String> stream = Streams.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
		Stream<Integer> s =
				stream
						.map(STRING_2_INTEGER)
						.filter(new Predicate<Integer>() {
							@Override
							public boolean test(Integer i) {
								return i % 2 == 0;
							}

						});

		await(2, s, is(4));
	}

	@Test
	public void testComposedErrorHandlingWithMultipleValues() throws InterruptedException {
		Stream<String> stream =
				Streams.defer(Arrays.asList("1", "2", "3", "4", "5"))
				       .env(env)
				       .dispatcher("eventLoop")
				       .get();

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
						.when(IllegalArgumentException.class, new Consumer<IllegalArgumentException>() {
							@Override
							public void accept(IllegalArgumentException e) {
								exception.set(true);
							}
						});

		await(5, s, is(10));
		assertThat("exception triggered", exception.get(), is(true));
	}

	@Test
	public void testReduce() throws InterruptedException {
		Stream<String> stream = Streams.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
		Stream<Integer> s =
				stream
						.map(STRING_2_INTEGER)
						.reduce(new Function<Tuple2<Integer, Integer>, Integer>() {
							@Override
							public Integer apply(Tuple2<Integer, Integer> r) {
								return r.getT1() * r.getT2();
							}
						}, 1);
		await(5, s, is(120));
	}

	@Test
	public void testFirstAndLast() throws InterruptedException {
		Stream<String> stream = Streams.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
		Stream<Integer> s =
				stream
						.map(STRING_2_INTEGER);

		Tap<Integer> first = s.first().tap();
		Tap<Integer> last = s.last().tap();

		s.flush();

		assertThat("First is 1", first.get(), is(1));
		assertThat("Last is 5", last.get(), is(5));
	}

	@Test
	public void testRelaysEventsToReactor() throws InterruptedException {
		Reactor r = Reactors.reactor().get();
		Selector key = Selectors.$();

		final CountDownLatch latch = new CountDownLatch(5);
		r.on(key, new Consumer<Event<Integer>>() {
			@Override
			public void accept(Event<Integer> integerEvent) {
				latch.countDown();
			}
		});

		Stream<String> stream = Streams.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
		Stream<Integer> s =
				stream
						.map(STRING_2_INTEGER)
						.consume(key.getObject(), r);
		Tap<Integer> tap = s.tap();

		s.flush(); // Trigger the deferred value to be set

		//await(s, is(5));
		assertThat("latch was counted down", latch.getCount(), is(0l));
		assertThat("value is 5", tap.get(), is(5));
	}

	@Test
	public void testStreamBatchesResults() {
		Stream<String> stream = Streams.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
		Stream<List<Integer>> s =
				stream
						.map(STRING_2_INTEGER)
						.collect();

		final AtomicInteger batchCount = new AtomicInteger();
		final AtomicInteger count = new AtomicInteger();
		s.consume(new Consumer<List<Integer>>() {
			@Override
			public void accept(List<Integer> is) {
				batchCount.incrementAndGet();
				for (int i : is) {
					count.addAndGet(i);
				}
			}
		}).flush();

		assertThat("batchCount is 3", batchCount.get(), is(1));
		assertThat("count is 15", count.get(), is(15));
	}

	@Test
	public void testHandlersErrorsDownstream() throws InterruptedException {
		Stream<String> stream = Streams.defer(Arrays.asList("1", "2", "a", "4", "5")).get();
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

		await(2, s, is(7));
		assertThat("error handler was invoked", latch.getCount(), is(0L));
	}

	@Test
	public void promiseAcceptCountCannotExceedOne() {
		Deferred<Object, Promise<Object>> deferred = Promises.<Object>defer().get();
		deferred.accept("alpha");
		try {
			deferred.accept("bravo");
		} catch (IllegalStateException ise) {
			// Swallow
		}
		assertEquals(deferred.compose().get(), "alpha");
	}

	@Test
	public void promiseErrorCountCannotExceedOne() {
		Deferred<Object, Promise<Object>> deferred = Promises.<Object>defer().get();
		Throwable error = new Exception();
		deferred.accept(error);
		try {
			deferred.accept(error);
		} catch (IllegalStateException ise) {
			// Swallow
		}
		assertTrue(deferred.compose().reason() instanceof Exception);
	}

	@Test
	public void promiseAcceptCountAndErrorCountCannotExceedOneInTotal() {
		Deferred<Object, Promise<Object>> deferred = Promises.<Object>defer().get();
		Throwable error = new Exception();
		deferred.accept(error);
		try {
			deferred.accept("alpha");
		} catch (IllegalStateException ise) {
			// Swallow
		}
		assertTrue(deferred.compose().reason() instanceof Exception);
		try {
			deferred.compose().get();
			fail();
		} catch (RuntimeException ise) {
			assertEquals(deferred.compose().reason(), ise.getCause());
		}
	}

	@Test
	public void mapManyFlushesAllValuesThoroughly() throws InterruptedException {
		int items = 30;
		CountDownLatch latch = new CountDownLatch(items);
		Random random = ThreadLocalRandom.current();

		Deferred<String, Stream<String>> d = Streams.defer(env, Environment.RING_BUFFER);
		Stream<Integer> tasks = d.compose()
		                         .mapMany(s -> Promises.success(s)
		                                               .env(env)
		                                               .dispatcher(Environment.THREAD_POOL)
		                                               .get()
		                                               .<Integer>map(str -> {
			                                               try {
				                                               Thread.sleep(random.nextInt(500));
			                                               } catch (InterruptedException e) {
				                                               Thread.currentThread().interrupt();
			                                               }
			                                               return Integer.parseInt(str);
		                                               }));

		tasks.consume(i -> latch.countDown());

		for (int i = 0; i < items; i++) {
			d.accept(String.valueOf(i));
		}

		assertTrue(latch.getCount() + " of " + items + " items were counted down",
		           latch.await(items, TimeUnit.SECONDS));
	}

	@Test
	public void mapManyFlushesAllValuesConsistently() throws InterruptedException {
		int iterations = 10;
		for (int i = 0; i < iterations; i++) {
			mapManyFlushesAllValuesThoroughly();
		}
	}

	<T> void await(Stream<T> s, Matcher<T> expected) throws InterruptedException {
		await(1, s, expected);
	}

	<T> void await(int count, Stream<T> s, Matcher<T> expected) throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(count);
		final AtomicReference<T> ref = new AtomicReference<T>();
		s.consume(new Consumer<T>() {
			@Override
			public void accept(T t) {
				ref.set(t);
				latch.countDown();
			}
		}).when(Exception.class, new Consumer<Exception>() {
			@Override
			public void accept(Exception e) {
				e.printStackTrace();
				latch.countDown();
			}
		}).flush();

		long startTime = System.currentTimeMillis();
		T result = null;
		try {
			latch.await(1, TimeUnit.SECONDS);
			result = ref.get();
		} catch (Exception e) {
			e.printStackTrace();
		}
		long duration = System.currentTimeMillis() - startTime;

		System.out.println(s.debug());
		assertThat(result, expected);
		assertThat(duration, is(lessThan(2000L)));
	}

	/**
	 * See #294 the consumer received more or less calls than expected Better reproducible with big thread pools, e.g. 128
	 * threads
	 *
	 * @throws InterruptedException
	 */
	@Test
	public void mapNotifiesOnce() throws InterruptedException {

		final int COUNT = 5000;
		final Object internalLock = new Object();
		final Object consumerLock = new Object();

		final CountDownLatch internalLatch = new CountDownLatch(COUNT);
		final CountDownLatch counsumerLatch = new CountDownLatch(COUNT);

		final AtomicInteger internalCounter = new AtomicInteger(0);
		final AtomicInteger consumerCounter = new AtomicInteger(0);

		final ConcurrentHashMap<Object, Long> seenInternal = new ConcurrentHashMap<>();
		final ConcurrentHashMap<Object, Long> seenConsumer = new ConcurrentHashMap<>();

		Environment e = new Environment();
		Deferred<Integer, Stream<Integer>> d = Streams.defer(e, e.getDispatcher("workQueue"));

		d.compose().map(new Function<Integer, Integer>() {
			@Override
			public Integer apply(Integer o) {
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
			}
		})
		 .consume(new Consumer<Integer>() {
			 @Override
			 public void accept(Integer o) {
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
			 }
		 });

		for (int i = 0; i < COUNT; i++) {
			d.accept(i);
		}

		assertTrue(internalLatch.await(5, TimeUnit.SECONDS));
		assertEquals(COUNT, internalCounter.get());
		assertTrue(counsumerLatch.await(5, TimeUnit.SECONDS));
		assertEquals(COUNT, consumerCounter.get());
	}

	static class String2Integer implements Function<String, Integer> {
		@Override
		public Integer apply(String s) {
			return Integer.parseInt(s);
		}
	}

}
