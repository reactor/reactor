package reactor.groovy

import org.junit.Test
import reactor.core.Promise
import reactor.fn.Consumer
import reactor.fn.Deferred
import reactor.fn.Function
import reactor.fn.Supplier
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static org.hamcrest.CoreMatchers.is
import static org.hamcrest.CoreMatchers.nullValue
import static org.hamcrest.MatcherAssert.assertThat


/**
 * @author Stephane Maldini (smaldini)
 */
class GroovyPromisesSpec extends Specification {


	/*@Test
	public void testPromiseNotifiesOfValues() throws InterruptedException {
		Promise<String> p = Promise.from("Hello World!");

		assertThat("Promise contains value", p.get(), is("Hello World!"));
	}

	@Test(expected = IllegalStateException.class)
	public void testPromiseNotifiesOfFailures() throws InterruptedException {
		Promise<String> p = Promise.from(new IllegalArgumentException("Bad code! Bad!"));

		assertThat("Promise is in failed state", p.getState(), is(Promise.State.FAILURE));
		assertThat("Promise has exploded", p.get(), is(nullValue()));
	}

	@Test
	public void testPromisesCanBeMapped() {
		Promise<String> p = Promise.sync();

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
		Promise<String> p = Promise.sync();

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
		Promise<String> p1 = Promise.sync();
		Promise<String> p2 = Promise.sync();

		p1.consume(p2);

		p1.set("Hello World!");

		assertThat("Second Promise is set", p2.get(), is("Hello World!"));
	}

	@Test
	public void testErrorsStopCompositions() throws InterruptedException {
		Promise<String> p = Promise.create();
		final CountDownLatch exceptionHandled = new CountDownLatch(1);

		Deferred<Integer> d = p
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

		assertThat("Supplier is not populated", d.await(500, TimeUnit.MILLISECONDS), is(nullValue()));
		assertThat("Exception has been handled", exceptionHandled.getCount(), is(0L));
	}

	@Test
	public void testPromiseComposesAfterSet() {
		Promise<String> p = Promise.sync("10");

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
	}*/

}
