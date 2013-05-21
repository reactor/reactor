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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.tcp.codec.Assembly;
import reactor.tcp.codec.Codec;
import reactor.tcp.codec.DecoderResult;
import reactor.tcp.codec.SimpleAssembly;
import reactor.tcp.data.Buffers;


/**
 * @author Gary Russell
 *
 */
public class TcpNioConnectionTests {

	@Test
	public void testRead() throws Exception {
		System.out.println(System.getProperty("java.class.path"));
		SocketChannel socketChannel = mock(SocketChannel.class);
		Socket socket = mock(Socket.class);
		when(socketChannel.socket()).thenReturn(socket);
		final AtomicReference<byte[]> foo = new AtomicReference<byte[]>("foo".getBytes());
		final AtomicInteger calls = new AtomicInteger();
		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				ByteBuffer bb = (ByteBuffer) invocation.getArguments()[0];
				bb.put(foo.get());
				calls.incrementAndGet();
				return null;
			}
		}).when(socketChannel).read(any(ByteBuffer.class));
		ConnectionFactorySupport connectionFactory = mock(ConnectionFactorySupport.class);
		TcpNioConnection connection = new TcpNioConnection(socketChannel, false, false, connectionFactory);
		final AtomicReference<DecoderResult> assembly = new AtomicReference<DecoderResult>();
		final CountDownLatch latch = new CountDownLatch(1);
		connection.registerListener(new TcpListener() {

			@Override
			public void onDecode(DecoderResult arg, TcpConnection connection) {
				assembly.set(arg);
				latch.countDown();
			}
		});
		connection.readPacket();
		assertEquals(1, calls.get());
		connection.readPacket();
		assertEquals(2, calls.get());
		foo.set("bar\n".getBytes());
		connection.readPacket();
		assertEquals(3, calls.get());
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertNotNull(assembly.get());
		assertEquals("foofoobar", new String(((Assembly) assembly.get()).asBytes()));
	}

	@Test
	public void testWrite() throws Exception {
		SocketChannel socketChannel = mock(SocketChannel.class);
		Socket socket = mock(Socket.class);
		when(socketChannel.socket()).thenReturn(socket);
		final ByteBuffer bytes = ByteBuffer.allocate(4);
		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				ByteBuffer buff = (ByteBuffer) invocation.getArguments()[0];
				byte[] bites = new byte[buff.remaining()];
				buff.get(bites);
				bytes.put(bites);
				return null;
			}
		}).when(socketChannel).write(any(ByteBuffer.class));
		ConnectionFactorySupport connectionFactory = mock(ConnectionFactorySupport.class);
		Selector selector = mock(Selector.class);
		when(connectionFactory.getIoSelector()).thenReturn(selector);
		TcpNioConnection connection = new TcpNioConnection(socketChannel, false, false, connectionFactory);
		connection.send("foo".getBytes());
		connection.doWrite(connection.getBuffersToWrite());
		byte[] out = new byte[4];
		bytes.flip();
		bytes.get(out);
		assertEquals("foo\n", new String(out));
	}

	@Test
	public void testPullCodec() throws Exception {
		Logger logger = LoggerFactory.getLogger(this.getClass());
		logger.info("foo");
		SocketChannel socketChannel = mock(SocketChannel.class);
		Socket socket = mock(Socket.class);
		when(socketChannel.socket()).thenReturn(socket);
		final AtomicReference<byte[]> foo = new AtomicReference<byte[]>("foo".getBytes());
		final AtomicInteger calls = new AtomicInteger();
		doAnswer(new Answer<Object>() {

			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				ByteBuffer bb = (ByteBuffer) invocation.getArguments()[0];
				bb.put(foo.get());
				calls.incrementAndGet();
				return null;
			}
		}).when(socketChannel).read(any(ByteBuffer.class));
		ConnectionFactorySupport connectionFactory = mock(ConnectionFactorySupport.class);
		TcpNioConnection connection = new TcpNioConnection(socketChannel, false, false, connectionFactory);
		final AtomicReference<DecoderResult> assembly = new AtomicReference<DecoderResult>();
		final CountDownLatch latch = new CountDownLatch(1);
		connection.registerListener(new TcpListener() {

			@Override
			public void onDecode(DecoderResult arg, TcpConnection connection) {
				assembly.set(arg);
				latch.countDown();
			}
		});
		connection.setCodec(new Codec() {

			@Override
			public void decode(Buffers buffers, DecoderCallback callback) {
				byte[] bytes = new byte[20];
				InputStream inputStream = buffers.getInputStream();
				int c;
				int n = 0;
				try {
					while ((c = inputStream.read()) >= 0) {
						if (c == 0x0a) {
							break;
						}
						bytes[n++] = (byte) c;
					}
					inputStream.close();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
				callback.complete(new SimpleAssembly(bytes, 0, n));
			}

			@Override
			public Buffers encode(ByteBuffer buffer) {
				return null;
			}
		});
		connection.readPacket();
		assertEquals(1, calls.get());
		connection.readPacket();
		assertEquals(2, calls.get());
		foo.set("bar\n".getBytes());
		connection.readPacket();
		assertEquals(3, calls.get());
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertNotNull(assembly.get());
		assertEquals("foofoobar", new String(((Assembly) assembly.get()).asBytes()));
	}
}
