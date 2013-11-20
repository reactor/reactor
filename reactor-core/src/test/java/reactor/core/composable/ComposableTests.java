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

import org.hamcrest.Matcher;
import org.junit.Test;
import reactor.AbstractReactorTest;
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
import reactor.operations.OperationUtils;
import reactor.tuple.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ComposableTests extends AbstractReactorTest {

	static final String2Integer STRING_2_INTEGER = new String2Integer();

	@Test
	public void testComposeFromSingleValue() throws InterruptedException {
		Deferred<String, Stream<String>> d = Streams.defer("Hello World!").get();
		Stream<String> s =
				d.compose()
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
		Deferred<String, Stream<String>> d = Streams.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
		Stream<Integer> s =
				d.compose()
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
		Deferred<String, Stream<String>> d = Streams.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
		Stream<Integer> s =
				d.compose()
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
		Deferred<String, Stream<String>> d =
				Streams.defer(Arrays.asList("1", "2", "3", "4", "5"))
				       .env(env)
				       .dispatcher("eventLoop")
				       .get();

		final AtomicBoolean exception = new AtomicBoolean(false);
		Stream<Integer> s =
				d.compose()
				 .map(STRING_2_INTEGER)
				 .when(IllegalArgumentException.class, new Consumer<IllegalArgumentException>() {
					 @Override
					 public void accept(IllegalArgumentException e) {
						  exception.set(true);
					 }
				 })
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
				 });

		await(5, s, is(10));
		assertThat("exception triggered", exception.get(), is(true));
	}

	@Test
	public void testReduce() throws InterruptedException {
		Deferred<String, Stream<String>> d = Streams.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
		Stream<Integer> s =
				d.compose()
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
		Deferred<String, Stream<String>> d = Streams.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
		Stream<Integer> s =
				d.compose()
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
		Tuple2<Selector, Object> key = Selectors.$();

		final CountDownLatch latch = new CountDownLatch(5);
		r.on(key.getT1(), new Consumer<Event<Integer>>() {
			@Override
			public void accept(Event<Integer> integerEvent) {
				latch.countDown();
			}
		});

		Deferred<String, Stream<String>> d = Streams.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
		Stream<Integer> s =
				d.compose()
				 .map(STRING_2_INTEGER)
				 .consume(key.getT2(), r);

		s.flush(); // Trigger the deferred value to be set

		await(s, is(5));
		assertThat("latch was counted down", latch.getCount(), is(0l));
	}

	@Test
	public void testStreamBatchesResults() {
		Deferred<String, Stream<String>> d = Streams.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
		Stream<List<Integer>> s =
				d.compose()
				 .map(STRING_2_INTEGER)
				 .batch(5)
				 .collect();

		final AtomicInteger batchCount = new AtomicInteger();
		final AtomicInteger count = new AtomicInteger();
		s.consume(new Consumer<List<Integer>>() {
			@Override
			public void accept(List<Integer> is) {
				batchCount.incrementAndGet();
				for(int i : is) {
					count.addAndGet(i);
				}
			}
		}).flush();

		assertThat("batchCount is 3", batchCount.get(), is(1));
		assertThat("count is 15", count.get(), is(15));
	}

	@Test
	public void testHandlersErrorsDownstream() throws InterruptedException {
		Deferred<String, Stream<String>> d = Streams.defer(Arrays.asList("1", "2", "a", "4", "5")).get();
		final CountDownLatch latch = new CountDownLatch(1);
		Stream<Integer> s =
				d.compose()
				 .map(STRING_2_INTEGER)
				 .map(new Function<Integer, Integer>() {
					 int sum = 0;

					 @Override
					 public Integer apply(Integer i) {
						 if(i >= 5) {
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
		} catch(IllegalStateException ise) {
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
		} catch(IllegalStateException ise) {
			// Swallow
		}
		assertTrue(deferred.compose().reason() instanceof IllegalStateException);
	}

	@Test
	public void promiseAcceptCountAndErrorCountCannotExceedOneInTotal() {
		Deferred<Object, Promise<Object>> deferred = Promises.<Object>defer().get();
		Throwable error = new Exception();
		deferred.accept(error);
		try {
			deferred.accept("alpha");
		} catch(IllegalStateException ise) {
			// Swallow
		}
		assertTrue(deferred.compose().reason() instanceof IllegalStateException);
		try{
			deferred.compose().get();
			fail();
		}catch(IllegalStateException ise){
			assertEquals(deferred.compose().reason(), ise);
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
		} catch(Exception e) {
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

}
