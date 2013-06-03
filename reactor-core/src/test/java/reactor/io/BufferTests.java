package reactor.io;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * @author Jon Brisbin
 */
public class BufferTests {

	@Test
	public void bufferSupportsSplittingOnDelimiter() {
		String data = "Hello World!\nHello World!\nHello World!\nHello World!\nHello ";
		Buffer buffer = Buffer.wrap(data);

		int buffers = 0;
		int newlines = 0;
		for (Buffer buff : buffer.split('\n')) {
			if (buff.last() == '\n') {
				newlines++;
			}
			buffers++;
		}

		assertThat("only 4 newlines were read", newlines, is(4));
		assertThat("all 5 buffers were found", buffers, is(5));
	}

}
