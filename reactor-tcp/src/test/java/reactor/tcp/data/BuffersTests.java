/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.tcp.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import reactor.tcp.codec.DefaultAssembly;
import reactor.tcp.data.Buffers;
import reactor.tcp.data.ByteIterator;

/**
 * @author Gary Russell
 *
 */
public class BuffersTests {

	@Test
	public void testBasic() {
		Buffers buffers = new Buffers();
		buffers.add(ByteBuffer.wrap("123".getBytes()));
		buffers.add(ByteBuffer.wrap("456".getBytes()));
		buffers.add(ByteBuffer.wrap("789".getBytes()));
		buffers.add(ByteBuffer.wrap("0ab".getBytes()));
		assertEquals(4, buffers.getBufferCount());
		assertEquals(12, buffers.getSize());
		DefaultAssembly assembly = new DefaultAssembly(buffers, 1);
		assertEquals("1", new String(assembly.asBytes()));
		assertEquals(4, buffers.getBufferCount());
		assertEquals(1, buffers.get(0).position());
		assertEquals(4, buffers.getBufferCount());
		assertEquals(11, buffers.getSize());
		assembly = new DefaultAssembly(buffers, 4);
		assertEquals("2345", new String(assembly.asBytes()));
		assertEquals(3, buffers.getBufferCount());
		assertEquals(2, buffers.get(0).position());
		assertEquals(3, buffers.getBufferCount());
		assertEquals(7, buffers.getSize());
		assembly = new DefaultAssembly(buffers, 7);
		assertEquals("67890ab", new String(assembly.asBytes()));
		assertEquals(0, buffers.getBufferCount());
		assertEquals(0, buffers.getBufferCount());
		assertEquals(0, buffers.getSize());
		ByteIterator iterator = assembly.iterator();
		byte[] out = new byte[7];
		int n = 0;
		while (iterator.hasNext()) {
			out[n++] = iterator.next();
		}
		assertEquals("67890ab", new String(out));
	}

	@Test
	public void testInputStream() throws Exception {
		Buffers buffers = new Buffers();
		buffers.add(ByteBuffer.wrap("123".getBytes()));
		buffers.add(ByteBuffer.wrap("456".getBytes()));
		buffers.add(ByteBuffer.wrap("789".getBytes()));
		buffers.add(ByteBuffer.wrap("0ab".getBytes()));
		final InputStream inputStream = buffers.getInputStream();
		ExecutorService exec = Executors.newSingleThreadExecutor();
		final CountDownLatch latch = new CountDownLatch(12);
		exec.execute(new Runnable() {

			@Override
			public void run() {
				while (true) {
					try {
						int c = inputStream.read();
						latch.countDown();
						if (c < 0) {
							System.err.print("EOF");
						}
						if (c == 0x0a) {
							System.out.print("LF");
							break;
						}
						System.out.print((char) c);
					}
					catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		});
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		buffers.add(ByteBuffer.wrap("cde".getBytes()));
		buffers.add(ByteBuffer.wrap("fgh\n".getBytes()));
		exec.shutdown();
		assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
	}
}
