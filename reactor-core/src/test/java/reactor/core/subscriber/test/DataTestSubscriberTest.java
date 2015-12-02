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
		DataTestSubscriber subscriber = DataTestSubscriber.createWithTimeoutSecs(1);
		Publishers.log(Publishers.map(Publishers.from(Arrays.asList("1", "2")), i -> Buffer.wrap("" + i)))
				.subscribe(subscriber);

		subscriber.request(1);

		subscriber.assertNextSignals("1");


		subscriber.request(1);

		subscriber.assertNextSignals("2");
	}

}