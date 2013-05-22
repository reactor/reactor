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

import reactor.tcp.codec.LineFeedCodecSupplier;
import reactor.tcp.test.TestUtils;

/**
 * @author Gary Russell
 *
 */
public class ConnectionFactoryTests {

	@Test
	public void testServer() throws Exception {
		int port = TestUtils.findAvailableServerSocket();

		TcpNioServerConnectionFactory<String> server = new TcpNioServerConnectionFactory<String>(port, new LineFeedCodecSupplier());
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<String> resultHolder = new AtomicReference<String>();
		server.registerListener(new TcpListener<String>() {

			@Override
			public void onDecode(String decoded, TcpConnection<String> connection) {
				resultHolder.set(decoded);
				latch.countDown();
			}
		});
		server.start();
		TestUtils.waitListening(server);
		Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
		socket.getOutputStream().write("foo\n".getBytes());
		socket.close();
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		server.stop();
		assertEquals("foo", resultHolder.get());
	}

	@Test
	public void testServerUnsolicited() throws Exception {
		int port = TestUtils.findAvailableServerSocket();
		TcpNioServerConnectionFactory<String> server = new TcpNioServerConnectionFactory<String>(port, new LineFeedCodecSupplier());
		server.registerListener(new ConnectionAwareTcpListener<String>() {

			@Override
			public void onDecode(String decoded, TcpConnection<String> connection) {
			}

			@Override
			public void newConnection(TcpConnection<String> connection) {
				try {
					connection.send("bar".getBytes());
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}

			@Override
			public void connectionClosed(TcpConnection<String> connection) {
			}
		});
		server.start();
		TestUtils.waitListening(server);
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
		TcpNioServerConnectionFactory<String> server = new TcpNioServerConnectionFactory<String>(port, new LineFeedCodecSupplier());
		server.registerListener(new TcpListener<String>() {

			@Override
			public void onDecode(String decoded, TcpConnection<String> connection) {
				System.out.println(decoded);
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
		TestUtils.waitListening(server);
		TcpNioClientConnectionFactory<String> client = new TcpNioClientConnectionFactory<String>("localhost", port, new LineFeedCodecSupplier());
		final CountDownLatch latch = new CountDownLatch(2);
		client.registerListener(new TcpListener<String>() {

			@Override
			public void onDecode(String decoded, TcpConnection<String> connection) {
				latch.countDown();
				System.out.println(decoded);
			}
		});
		client.start();
		TcpConnectionSupport<String> connection = client.getConnection();
		connection.send("foo\nfoo".getBytes());
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		client.stop();
		server.stop();
	}

	@Test
	public void testClientAndServerSSL() throws Exception {
		System.setProperty("javax.net.debug", "all"); // SSL activity in the console
		int port = TestUtils.findAvailableServerSocket();
		TcpNioServerConnectionFactory<String> server = new TcpNioServerConnectionFactory<String>(port, new LineFeedCodecSupplier());
		DefaultTcpSSLContextConfigurer sslContextConfigurer = new DefaultTcpSSLContextConfigurer(new File("src/test/resources/test.ks"),
				new File("src/test/resources/test.truststore.ks"), "secret", "secret");
		sslContextConfigurer.setProtocol("SSL");
		DefaultTcpNioSSLConnectionConfigurer tcpNioConnectionSupport = new DefaultTcpNioSSLConnectionConfigurer(sslContextConfigurer);
		server.setTcpNioConnectionSupport(tcpNioConnectionSupport);
		final AtomicReference<String> resultHolder = new AtomicReference<String>();
		final CountDownLatch latch = new CountDownLatch(1);
		server.registerListener(new TcpListener<String>() {

			@Override
			public void onDecode(String decoded, TcpConnection<String> connection) {
				resultHolder.set(decoded);
				latch.countDown();
			}
		});
		server.start();
		TestUtils.waitListening(server);

		TcpNioClientConnectionFactory<String> client = new TcpNioClientConnectionFactory<String>("localhost", port, new LineFeedCodecSupplier());
		client.setTcpNioConnectionSupport(tcpNioConnectionSupport);
		client.registerListener(new TcpListener<String>() {

			@Override
			public void onDecode(String result, TcpConnection<String> connection) {
				System.out.println("Client " + result);
			}
		});
		client.start();

		TcpConnection<String> connection = client.getConnection();
		connection.send("Hello, world!".getBytes());
		assertTrue(latch.await(10, TimeUnit.SECONDS));
		assertEquals("Hello, world!", resultHolder.get());
	}


}
