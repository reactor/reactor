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

import org.junit.Test;
import reactor.AbstractReactorTest;
import reactor.Fn;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.fn.Supplier;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class PromiseTests extends AbstractReactorTest {

	@Test
	public void testPromiseNotifiesOfValues() throws InterruptedException {
		Promise<String> p = R.success("Hello World!").get();
		assertThat("Promise is in success state", p.isSuccess(), is(true));
		assertThat("Promise contains value", p.get(), is("Hello World!"));
	}

	@Test(expected = IllegalStateException.class)
	public void testPromiseNotifiesOfFailures() throws InterruptedException {
		Promise<String> p = R.<String>error(new IllegalArgumentException("Bad code! Bad!")).get();
		assertThat("Promise is in failed state", p.isError(), is(true));
		assertThat("Promise has exploded", p.get(), is(nullValue()));
	}

	@Test
	public void testPromisesCanBeMapped() {
		Promise<String> p = R.<String>promise().sync().get();

		Supplier<Integer> s = p.map(new Function<String, Integer>() {
			@Override
			public Integer apply(String s) {
				return Integer.parseInt(s);
			}
		});

		p.set("10");

		assertThat("Transformation has occurred", s.get(), is(10));
	}

	@Test
	public void testPromisesCanBeFiltered() {
		Promise<String> p = R.<String>promise().sync().get();

		Supplier<Integer> s = p
				.map(new Function<String, Integer>() {
					@Override
					public Integer apply(String s) {
						return Integer.parseInt(s);
					}
				})
				.filter(new Function<Integer, Boolean>() {
					@Override
					public Boolean apply(Integer integer) {
						return integer > 10;
					}
				});

		p.set("10");

		assertThat("Value is null because it failed a filter", s.get(), is(nullValue()));
	}

	@Test
	public void testPromiseAsConsumer() {
		Promise<String> p1 = R.<String>promise().sync().get();
		Promise<String> p2 = R.<String>promise().sync().get();

		p1.consume(p2);

		p1.set("Hello World!");

		assertThat("Second Promise is set", p2.get(), is("Hello World!"));
	}

	@Test
	public void testErrorsStopCompositions() throws InterruptedException {
		Promise<String> p = R.<String>promise().get();
		final CountDownLatch exceptionHandled = new CountDownLatch(1);

		Composable<Integer> c = p
				.map(new Function<String, Integer>() {
					@Override
					public Integer apply(String s) {
						return Integer.parseInt(s);
					}
				})
				.when(NumberFormatException.class, new Consumer<NumberFormatException>() {
					@Override
					public void accept(NumberFormatException nfe) {
						exceptionHandled.countDown();
					}
				})
				.filter(new Function<Integer, Boolean>() {
					@Override
					public Boolean apply(Integer integer) {
						System.out.println("This should not appear in the log...");
						return true;
					}
				});

		p.set("Not A Number");

		boolean ex = false;
		try{
			c.await(500, TimeUnit.MILLISECONDS);
		}catch (IllegalStateException nfe){
			ex = true;
		}
		assertThat("Supplier is not populated", ex);
		assertThat("Exception has been handled", exceptionHandled.getCount(), is(0L));
	}

	@Test
	public void testPromiseComposesAfterSet() {
		Promise<String> p = R.success("10").get();

		Supplier<Integer> s = p
				.map(new Function<String, Integer>() {
					@Override
					public Integer apply(String s) {
						return Integer.parseInt(s);
					}
				})
				.map(new Function<Integer, Integer>() {
					@Override
					public Integer apply(Integer integer) {
						return integer * 10;
					}
				});

		assertThat("Promise has provided the value to the composition", s.get(), is(100));
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void promiseCanBeFulfilledFromASeparateThread() throws InterruptedException {
		Reactor r1 = R.reactor().get();
		Reactor r2 = R.reactor().using(env).threadPoolExecutor().get();

		final Promise<String> promise = R.<String>promise().using(r1).get();
		final CountDownLatch latch = new CountDownLatch(1);

		Fn.schedule(new Consumer() {

			@Override
			public void accept(Object t) {
				promise.set("foo");
			}

		}, null, r2);

		promise.onSuccess(new Consumer<String>() {

			@Override
			public void accept(String t) {
				latch.countDown();
			}
		});

		assertTrue(latch.await(5, TimeUnit.SECONDS));
	}

}
