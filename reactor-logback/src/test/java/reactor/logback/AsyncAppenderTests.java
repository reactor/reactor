package reactor.logback;

import ch.qos.logback.classic.Logger;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

/**
 * @author Jon Brisbin
 */
public class AsyncAppenderTests {

	static final String MSG;

	static {
		String ABCS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
		Random r = new Random();

		char[] chars = new char[20000];
		int len = chars.length;
		for (int i = 0; i < len; i++) {
			chars[i] = ABCS.charAt(r.nextInt(ABCS.length()));
		}
		MSG = new String(chars);
	}

	@Test
	public void asyncAppenderLogsAsynchronously() {
		Logger log = (Logger)LoggerFactory.getLogger(AsyncAppenderTests.class);

		log.info(MSG);


		assertThat("Appender is not set", log.iteratorForAppenders().next(), instanceOf(AsyncAppender.class));
	}

}
