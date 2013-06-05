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
