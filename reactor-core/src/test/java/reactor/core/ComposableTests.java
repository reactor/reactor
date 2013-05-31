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

package reactor.core;

import org.hamcrest.Matcher;
import org.junit.Test;
import reactor.AbstractReactorTest;
import reactor.fn.*;
import reactor.fn.selector.Selector;
import reactor.fn.support.Reduce;
import reactor.fn.tuples.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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
		Composable<String> c = Composables.init("Hello World!").get();

		Composable<String> d = c.map(new Function<String, String>() {
			@Override
			public String apply(String s) {
				return "Goodbye then!";
			}
		});

		await(d, is("Goodbye then!"));
	}

	@Test
	public void testComposeFromMultipleValues() throws InterruptedException {
		Composable<Integer> c = Composables
				.each(Arrays.asList("1", "2", "3", "4", "5"))
				.get()
				.map(STRING_2_INTEGER)
				.map(new Function<Integer, Integer>() {
					int sum = 0;

					@Override
					public Integer apply(Integer i) {
						sum += i;
						return sum;
					}
				});

		await(c, is(15));
	}

	@Test
	public void testComposeFromMultipleFilteredValues() throws InterruptedException {
		Composable<Integer> c = Composables
				.each(Arrays.asList("1", "2", "3", "4", "5"))
				.get()
				.map(STRING_2_INTEGER)
				.filter(new Function<Integer, Boolean>() {

					@Override
					public Boolean apply(Integer t) {
						return t % 2 == 0;
					}

				});

		await(c, is(4));
	}

	@Test
	public void testComposedErrorHandlingWithMultipleValues() throws InterruptedException {
		Composable<Integer> c = Composables
				.each(Arrays.asList("1", "2", "3", "4", "5"))
				.using(env)
				.eventLoop()
				.get()
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

		await(c, is(10));
	}

	@Test
	public void valueIsImmediatelyAvailable() throws InterruptedException {
		Composable<String> c = Composables.each(Arrays.asList("1", "2", "3", "4", "5")).get();

		await(c, is("5"));
	}

	@Test
	public void testReduce() throws InterruptedException {
		Composable<Integer> c = Composables
				.each(Arrays.asList("1", "2", "3", "4", "5"))
				.get()
				.map(STRING_2_INTEGER)
				.reduce(new Function<Reduce<Integer, Integer>, Integer>() {
					@Override
					public Integer apply(Reduce<Integer, Integer> r) {
						return ((null != r.getLastValue() ? r.getLastValue() : 1) * r.getNextValue());
					}
				});

		await(c, is(120));
	}

	@Test
	public void testFirstAndLast() throws InterruptedException {
		Composable<Integer> c = Composables
				.each(Arrays.asList("1", "2", "3", "4", "5"))
				.get()
				.map(STRING_2_INTEGER);

		Composable<Integer> first = c.first();
		Composable<Integer> last = c.last();

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

		Composable<Integer> c = Composables
				.each(Arrays.asList("1", "2", "3", "4", "5"))
				.get()
				.map(STRING_2_INTEGER)
				.consume(key.getT2(), r);

		c.get(); // Trigger the deferred value to be set

		latch.await(1, TimeUnit.SECONDS);
		assertThat(latch.getCount(), is(0L));
		assertThat(c.get(), is(5));
	}

	@Test
	public void composableWithInitiallyUnknownNumberOfValues() throws InterruptedException {
		final Composable<Integer> c = Composables
				.each(new TestIterable<String>("1", "2", "3", "4", "5"))
				.get()
				.map(STRING_2_INTEGER)
				.map(new Function<Integer, Integer>() {
					int sum = 0;

					@Override
					public Integer apply(Integer i) {
						sum += i;
						return sum;
					}
				});

		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {

				}
				c.setExpectedAcceptCount(5);
			}
		}).start();

		await(c, is(15));
	}

	<T> void await(Composable<T> d, Matcher<T> expected) throws InterruptedException {
		long startTime = System.currentTimeMillis();
		T result = null;
		try {
			result = d.await(1, TimeUnit.SECONDS);
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

	static class TestIterable<T> implements Iterable<T> {

		private final Collection<T> items;

		@SafeVarargs
		public TestIterable(T... items) {
			this.items = Arrays.asList(items);
		}

		@Override
		public Iterator<T> iterator() {
			return this.items.iterator();
		}

	}

}
