package reactor.core.subscriber.test;

import org.junit.Test;
import reactor.Publishers;
import reactor.io.buffer.Buffer;

import java.util.Arrays;

/**
 * @author Anatoly Kadyshev
 */
public class DataTestSubscriberTest {

	@Test
	public void testAssertNextSignals() throws Exception {
		DataTestSubscriber<String> subscriber = DataTestSubscriber.createWithTimeoutSecs(1);
		Publishers.log(Publishers.from(Arrays.asList("1", "2")))
				.subscribe(subscriber);

		subscriber.request(1);

		subscriber.assertNextSignals("1");


		subscriber.request(1);

		subscriber.assertNextSignals("2");
	}

}