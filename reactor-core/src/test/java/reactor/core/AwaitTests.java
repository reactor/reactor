package reactor.core;

import org.junit.Test;
import reactor.AbstractReactorTest;
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
public class AwaitTests extends AbstractReactorTest {

	@Test
	public void testAwaitDoesntBlockUnnecessarily() throws InterruptedException {
		ThreadPoolExecutorDispatcher dispatcher = new ThreadPoolExecutorDispatcher(4, 64);
		Reactor reactor = R.reactor().using(env).threadPoolExecutor().get();
		Reactor innerReactor = R.reactor().using(env).dispatcher(dispatcher).get();
		for (int i = 0; i < 1000; i++) {
			final Promise<String> promise = R.<String>promise().using(env).using(reactor).get();
			final CountDownLatch latch = new CountDownLatch(1);

			promise.onSuccess(new Consumer<String>() {

				@Override
				public void accept(String t) {
					latch.countDown();
				}
			});
			Fn.schedule(new Consumer() {

				@Override
				public void accept(Object t) {
					promise.set("foo");

				}

			}, null, innerReactor);

			assertThat("latch is counted down", latch.await(5, TimeUnit.SECONDS));
		}
	}

}
