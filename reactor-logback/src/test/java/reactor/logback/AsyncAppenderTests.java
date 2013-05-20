package reactor.logback;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
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
		Logger log = LoggerFactory.getLogger(AsyncAppenderTests.class);

		long start = System.currentTimeMillis();
		log.info(MSG);
		long end = System.currentTimeMillis();

		assertThat("Time for call is too short to have done blocking IO", (end - start), lessThan(25L));
	}

}
