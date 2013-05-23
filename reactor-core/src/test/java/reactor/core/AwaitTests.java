package reactor.core;

import org.junit.Test;
import reactor.Fn;
import reactor.fn.Consumer;
import reactor.fn.dispatch.ThreadPoolExecutorDispatcher;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Jon Brisbin
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class AwaitTests {

	@Test
	public void testAwaitDoesntBlockUnnecessarily() throws InterruptedException {
		ThreadPoolExecutorDispatcher dispatcher = new ThreadPoolExecutorDispatcher(4, 64).start();
		Reactor reactor = new Reactor();
		Reactor innerReactor = new Reactor(dispatcher);
		for (int i = 0; i < 1000; i++) {
			final Promise<String> promise = new Promise<String>(reactor);
			final CountDownLatch latch = new CountDownLatch(1);

			Fn.schedule(new Consumer() {

				@Override
				public void accept(Object t) {
					promise.set("foo");

				}

			}, null, innerReactor);

			promise.onSuccess(new Consumer<String>() {

				@Override
				public void accept(String t) {
					latch.countDown();
				}
			});

			assertThat("latch is counted down", latch.await(5, TimeUnit.SECONDS));
		}
	}
}
