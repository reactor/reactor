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
import org.junit.Test;
import reactor.AbstractReactorTest;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.Reactors;
import reactor.event.Event;
import reactor.event.selector.Selector;
import reactor.event.selector.Selectors;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.function.Predicate;
import reactor.function.support.Tap;
import reactor.rx.spec.Promises;
import reactor.rx.spec.Streams;
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
public class PipelineTests extends AbstractReactorTest {

	static final String2Integer STRING_2_INTEGER = new String2Integer();

	@Test
	public void testComposeFromSingleValue() throws InterruptedException {
		Stream<String> stream = Streams.defer("Hello World!");
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
		Stream<String> stream = Streams.defer(Arrays.asList("1", "2", "3", "4", "5"));
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
		Stream<String> stream = Streams.defer(Arrays.asList("1", "2", "3", "4", "5"));
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
				Streams.defer(Arrays.asList("1", "2", "3", "4", "5"), env, env.getDispatcher("eventLoop"));

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
		Stream<String> stream = Streams.defer(Arrays.asList("1", "2", "3", "4", "5"));
		Stream<Integer> s =
				stream
						.map(STRING_2_INTEGER)
						.reduce(new Function<Tuple2<Integer, Integer>, Integer>() {
							@Override
							public Integer apply(Tuple2<Integer, Integer> r) {
								return r.getT1() * r.getT2();
							}
						}, 1);
		await(1, s, is(120));
	}

	@Test
	public void testFirstAndLast() throws InterruptedException {
		Stream<String> stream = Streams.defer(Arrays.asList("1", "2", "3", "4", "5"));
		Stream<Integer> s =
				stream
						.map(STRING_2_INTEGER);

		Stream<Integer> first = s.first();
		Stream<Integer> last = s.last();

		System.out.println(s.debug());

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

		Stream<String> stream = Streams.defer(Arrays.asList("1", "2", "3", "4", "5"));
		Stream<Integer> s =
				stream
						.map(STRING_2_INTEGER)
						.consume(key.getObject(), r);
		System.out.println(s.debug());

		//await(s, is(5));
		assertThat("latch was counted down", latch.getCount(), is(0l));
		assertThat("value is 5", tap.get().getData(), is(5));
	}

	@Test
	public void testStreamBatchesResults() {
		Stream<String> stream = Streams.defer(Arrays.asList("1", "2", "3", "4", "5"));
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
		});

		assertThat("batchCount is 3", batchCount.get(), is(1));
		assertThat("count is 15", count.get(), is(15));
	}

	@Test
	public void testHandlersErrorsDownstream() throws InterruptedException {
		Stream<String> stream = Streams.defer(Arrays.asList("1", "2", "a", "4", "5"));
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
		deferred.broadcastNext("alpha");
		try {
			deferred.broadcastNext("bravo");
		} catch (IllegalStateException ise) {
			// Swallow
		}
		assertEquals(deferred.get(), "alpha");
	}

	@Test
	public void promiseErrorCountCannotExceedOne() {
		Promise<Object> deferred = Promises.defer();
		Throwable error = new Exception();
		deferred.broadcastError(error);
		try {
			deferred.broadcastNext(error);
		} catch (IllegalStateException ise) {
			// Swallow
		}
		assertTrue(deferred.reason() instanceof Exception);
	}

	@Test
	public void promiseAcceptCountAndErrorCountCannotExceedOneInTotal() {
		Promise<Object> deferred = Promises.defer();
		Throwable error = new Exception();
		deferred.broadcastError(error);
		try {
			deferred.broadcastNext("alpha");
			fail();
		} catch (IllegalStateException ise) {
		}
		assertTrue(deferred.reason() instanceof Exception);
	}

	@Test
	public void mapManyFlushesAllValuesThoroughly() throws InterruptedException {
		int items = 30;
		CountDownLatch latch = new CountDownLatch(items);
		Random random = ThreadLocalRandom.current();

		Stream<String> d = Streams.defer(env);
		Stream<Integer> tasks = d.mapMany(s -> Promises.success(s, env, env.getDispatcher(Environment.THREAD_POOL))
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
			d.broadcastNext(String.valueOf(i));
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

	<T> void await(int count, final Stream<T> s, Matcher<T> expected) throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(count);
		final AtomicReference<T> ref = new AtomicReference<T>();
		s.when(Exception.class, new Consumer<Exception>() {
			@Override
			public void accept(Exception e) {
				e.printStackTrace();
				latch.countDown();
			}
		}).consume(new Consumer<T>() {
			@Override
			public void accept(T t) {
				System.out.println(s.debug());
				ref.set(t);
				latch.countDown();
			}
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
			System.out.println(s);
			return Integer.parseInt(s);
		}
	}

}
