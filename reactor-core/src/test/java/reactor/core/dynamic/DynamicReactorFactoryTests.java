package reactor.core.dynamic;

import org.junit.Test;
import reactor.fn.Consumer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Jon Brisbin
 */
public class DynamicReactorFactoryTests {

	@Test
	public void testCreatesDynamicReactors() throws InterruptedException {
		MyReactor myReactor = new DynamicReactorFactory<MyReactor>(MyReactor.class).create();

		final CountDownLatch latch = new CountDownLatch(2);
		myReactor.
								 onTest(new Consumer<String>() {
									 @Override
									 public void accept(String s) {
										 latch.countDown();
									 }
								 }).
								 onTestTest(new Consumer<String>() {
									 @Override
									 public void accept(String s) {
										 latch.countDown();
									 }
								 }).
								 notifyTest("Hello World!").
								 notifyTestTest("Hello World!");

		latch.await(5, TimeUnit.SECONDS);

		assertThat("Latch has been counted down", latch.getCount() == 0);
	}

}
