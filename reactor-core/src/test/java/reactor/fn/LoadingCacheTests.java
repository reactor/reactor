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

package reactor.fn;

import org.junit.Test;
import reactor.fn.cache.LoadingCache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author Jon Brisbin
 */
public class LoadingCacheTests {

	@Test
	public void loadingCacheWarmsCache() {
		final int bufferCount = 100;
		final long timeout = 1000L;

		LoadingCache<ByteBuffer> bufferCache = new LoadingCache<ByteBuffer>(
				new Supplier<ByteBuffer>() {
					@Override
					public ByteBuffer get() {
						return ByteBuffer.allocate(8 * 1024);
					}
				},
				bufferCount,
				timeout
		);

		// exhaust cache
		List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(100);
		for (int i = 0; i < bufferCount + 1; i++) {
			buffers.add(bufferCache.allocate());
		}

		long start = System.currentTimeMillis();
		ByteBuffer b = bufferCache.allocate();
		long end = System.currentTimeMillis();

		assertThat("ByteBuffer was obtained despite cache exhaustion", b, is(notNullValue()));
		assertThat("Cache miss timeout was exceeded", end - start, is(greaterThanOrEqualTo(timeout)));
	}

}
