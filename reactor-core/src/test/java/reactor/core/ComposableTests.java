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

package reactor.core;

import org.hamcrest.Matcher;
import org.junit.Test;
import reactor.AbstractReactorTest;
import reactor.R;
import reactor.S;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.Function;
import reactor.fn.Predicate;
import reactor.fn.selector.Selector;
import reactor.fn.tuples.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static reactor.Fn.$;

/**
 * @author Jon Brisbin
 */
public class ComposableTests extends AbstractReactorTest {

	static final String2Integer STRING_2_INTEGER = new String2Integer();

	@Test
	public void testComposeFromSingleValue() throws InterruptedException {
		Deferred<String, Stream<String>> d = S.defer("Hello World!").get();
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
		Deferred<String, Stream<String>> d = S.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
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
		Deferred<String, Stream<String>> d = S.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
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
				S.defer(Arrays.asList("1", "2", "3", "4", "5"))
				 .using(env)
				 .dispatcher("eventLoop")
				 .get();
		Stream<Integer> s =
				d.compose()
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
				 });

		await(4, s, is(10));
		assertThat("error count is 1", s.getErrorCount(), is(1L));
	}

	@Test
	public void testReduce() throws InterruptedException {
		Deferred<String, Stream<String>> d = S.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
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
		Deferred<String, Stream<String>> d = S.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
		Stream<Integer> s =
				d.compose()
				 .map(STRING_2_INTEGER);

		Stream<Integer> first = s.first();
		Stream<Integer> last = s.last();

		await(first, is(1));
		await(last, is(5));
	}

	@Test
	public void testRelaysEventsToReactor() throws InterruptedException {
		Reactor r = R.reactor().get();
		Tuple2<Selector, Object> key = $();

		final CountDownLatch latch = new CountDownLatch(5);
		r.on(key.getT1(), new Consumer<Event<Integer>>() {
			@Override
			public void accept(Event<Integer> integerEvent) {
				latch.countDown();
			}
		});

		Deferred<String, Stream<String>> d = S.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
		Stream<Integer> s =
				d.compose()
				 .map(STRING_2_INTEGER)
				 .consume(key.getT2(), r);

		s.get(); // Trigger the deferred value to be set

		await(s, is(5));
		assertThat("latch was counted down", latch.getCount(), is(0l));
	}

	@Test
	public void testStreamBatchesResults() {
		Deferred<String, Stream<String>> d = S.defer(Arrays.asList("1", "2", "3", "4", "5")).get();
		Stream<List<Integer>> s =
				d.compose()
				 .map(STRING_2_INTEGER)
				 .batch(2);

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
		}).get();

		assertThat("batchCount is 3", batchCount.get(), is(3));
		assertThat("count is 15", count.get(), is(15));
	}

	@Test
	public void testHandlersErrorsDownstream() throws InterruptedException {
		Deferred<String, Stream<String>> d = S.defer(Arrays.asList("1", "2", "a", "4", "5")).get();
		final CountDownLatch latch = new CountDownLatch(1);
		Stream<Integer> s =
				d.compose()
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
		}).get();

		long startTime = System.currentTimeMillis();
		T result = null;
		try {
			latch.await(1, TimeUnit.SECONDS);
			result = ref.get();
		} catch (Exception e) {
		}
		long duration = System.currentTimeMillis() - startTime;

		assertThat(result, expected);
		assertThat(duration, is(lessThan(1000L)));
	}

	static class String2Integer implements Function<String, Integer> {
		@Override
		public Integer apply(String s) {
			return Integer.parseInt(s);
		}
	}

}
