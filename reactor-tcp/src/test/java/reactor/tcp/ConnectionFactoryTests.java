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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.SocketFactory;

import org.junit.Test;

import reactor.tcp.codec.Assembly;
import reactor.tcp.codec.DecoderResult;
import reactor.tcp.test.TestUtils;

/**
 * @author Gary Russell
 * @since 3.0
 *
 */
public class ConnectionFactoryTests {

	@Test
	public void testServer() throws Exception {
		int port = TestUtils.findAvailableServerSocket();
		TcpNioServerConnectionFactory server = new TcpNioServerConnectionFactory(port);
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<DecoderResult> assy = new AtomicReference<DecoderResult>();
		server.registerListener(new TcpListener() {

			@Override
			public void onDecode(DecoderResult assembly, TcpConnection connection) {
				assy.set(assembly);
				latch.countDown();
			}
		});
		server.start();
		TestUtils.waitListening(server, null);
		Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
		socket.getOutputStream().write("foo\n".getBytes());
		socket.close();
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		server.stop();
		assertEquals("foo", new String(((Assembly) assy.get()).asBytes()));
	}

	@Test
	public void testServerUnsolicited() throws Exception {
		int port = TestUtils.findAvailableServerSocket();
		TcpNioServerConnectionFactory server = new TcpNioServerConnectionFactory(port);
		server.registerListener(new ConnectionAwareTcpListener() {

			@Override
			public void onDecode(DecoderResult assembly, TcpConnection connection) {
			}

			@Override
			public void newConnection(TcpConnection connection) {
				try {
					connection.send("bar".getBytes());
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}

			@Override
			public void connectionClosed(TcpConnection connection) {
			}
		});
		server.start();
		TestUtils.waitListening(server, null);
		Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
		socket.setSoTimeout(10000);
		ByteBuffer bb = ByteBuffer.allocate(10);
		int c;
		while ((c = socket.getInputStream().read()) != '\n') {
			bb.put((byte) c);
		}
		socket.close();
		server.stop();
		assertEquals(3, bb.position());
		bb.flip();
		byte[] unsolicited =  new byte[3];
		bb.get(unsolicited);
		assertEquals("bar", new String(unsolicited));
	}

	@Test
	public void testClientAndServer() throws Exception {
		int port = TestUtils.findAvailableServerSocket();
		TcpNioServerConnectionFactory server = new TcpNioServerConnectionFactory(port);
		server.registerListener(new TcpListener() {

			@Override
			public void onDecode(DecoderResult assembly, TcpConnection connection) {
				System.out.println(assembly);
				try {
					connection.send("bar".getBytes());
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
//		server.setSoTimeout(2000);
		server.start();
		TestUtils.waitListening(server, null);
		TcpNioClientConnectionFactory client = new TcpNioClientConnectionFactory("localhost", port);
		final CountDownLatch latch = new CountDownLatch(2);
		client.registerListener(new TcpListener() {

			@Override
			public void onDecode(DecoderResult assembly, TcpConnection connection) {
				latch.countDown();
				System.out.println(assembly);
			}
		});
		client.start();
		TcpConnectionSupport connection = client.getConnection();
		connection.send("foo\nfoo".getBytes());
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		client.stop();
		server.stop();
	}

	@Test
	public void testClientAndServerSSL() throws Exception {
		System.setProperty("javax.net.debug", "all"); // SSL activity in the console
		int port = TestUtils.findAvailableServerSocket();
		TcpNioServerConnectionFactory server = new TcpNioServerConnectionFactory(port);
		DefaultTcpSSLContextConfigurer sslContextConfigurer = new DefaultTcpSSLContextConfigurer(new File("src/test/resources/test.ks"),
				new File("src/test/resources/test.truststore.ks"), "secret", "secret");
		sslContextConfigurer.setProtocol("SSL");
		DefaultTcpNioSSLConnectionConfigurer tcpNioConnectionSupport = new DefaultTcpNioSSLConnectionConfigurer(sslContextConfigurer);
		server.setTcpNioConnectionSupport(tcpNioConnectionSupport);
		final AtomicReference<DecoderResult> assembly = new AtomicReference<DecoderResult>();
		final CountDownLatch latch = new CountDownLatch(1);
		server.registerListener(new TcpListener() {

			@Override
			public void onDecode(DecoderResult assy, TcpConnection connection) {
				assembly.set(assy);
				latch.countDown();
			}
		});
		server.start();
		TestUtils.waitListening(server, null);

		TcpNioClientConnectionFactory client = new TcpNioClientConnectionFactory("localhost", port);
		client.setTcpNioConnectionSupport(tcpNioConnectionSupport);
		client.registerListener(new TcpListener() {

			@Override
			public void onDecode(DecoderResult assembly, TcpConnection connection) {
				System.out.println("Client " + assembly);
			}
		});
		client.start();

		TcpConnection connection = client.getConnection();
		connection.send("Hello, world!".getBytes());
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertEquals("Hello, world!", new String(((Assembly) assembly.get()).asBytes()));
	}


}
