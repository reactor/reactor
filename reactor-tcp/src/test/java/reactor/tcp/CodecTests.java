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
package reactor.tcp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import reactor.tcp.codec.Assembly;
import reactor.tcp.codec.Codec.DecoderCallback;
import reactor.tcp.codec.DecodedObject;
import reactor.tcp.codec.DecoderResult;
import reactor.tcp.codec.DefaultAssembly;
import reactor.tcp.codec.JavaSerializationCodec;
import reactor.tcp.codec.LineFeedCodec;

/**
 * @author Gary Russell
 * @since 3.0
 *
 */
public class CodecTests {

	@Test
	public void testLFEncode() {
		LineFeedCodec codec = new LineFeedCodec();
		ByteBuffer data = ByteBuffer.wrap("foo".getBytes());
		Buffers out = codec.encode(data);
		Iterator<ByteBuffer> iterator = out.iterator();
		assertEquals(data, iterator.next());
		ByteBuffer lfBuffer = iterator.next();
		byte lf = lfBuffer.get();
		assertEquals(0, lfBuffer.remaining());
		assertEquals(0x0a, lf);
	}

	@Test
	public void testLFDecode() {
		LineFeedCodec codec = new LineFeedCodec();
		Buffers buffers = new Buffers();
		buffers.add(ByteBuffer.wrap("123".getBytes()));
		buffers.add(ByteBuffer.wrap("456".getBytes()));
		buffers.add(ByteBuffer.wrap("789".getBytes()));
		buffers.add(ByteBuffer.wrap("0ab".getBytes()));
		final AtomicReference<DecoderResult> assembly = new AtomicReference<DecoderResult>();
		codec.decode(buffers, new DecoderCallback() {

			@Override
			public void complete(DecoderResult result) {
				assembly.set(result);
			}
		});
		buffers.add(ByteBuffer.wrap("foo\nbar".getBytes()));
		codec.decode(buffers, new DecoderCallback() {

			@Override
			public void complete(DecoderResult result) {
				assembly.set(result);
			}
		});
		assertNotNull(assembly);
		assertEquals("1234567890abfoo", new String(((Assembly) assembly.get()).asBytes()));
		assertEquals(1, buffers.getBufferCount());
		Assembly assy = new DefaultAssembly(buffers, 3);
		assertEquals("bar", new String(assy.asBytes()));
		assertEquals(0, buffers.getBufferCount());
	}


	@Test
	public void testJavaEncodeDecode() throws Exception {
		String foo = "foo";
		JavaSerializationCodec codec = new JavaSerializationCodec();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		codec.encode(foo, baos);

		Buffers buffers = new Buffers();
		ByteBuffer bb = ByteBuffer.wrap(baos.toByteArray());
		buffers.add(bb);
		final AtomicReference<DecodedObject> result = new AtomicReference<DecodedObject>();
		final CountDownLatch latch = new CountDownLatch(1);
		codec.decode(buffers, new DecoderCallback() {

			@Override
			public void complete(DecoderResult r) {
				result.set((DecodedObject) r);
				latch.countDown();
			}
		});
		assertTrue(latch.await(10,  TimeUnit.SECONDS));
		assertEquals("foo", result.get().getObject());
	}

}
