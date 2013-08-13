/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

package reactor.tcp;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Environment;
import reactor.core.composable.Promise;
import reactor.function.Consumer;
import reactor.io.Buffer;
import reactor.tcp.encoding.StandardCodecs;
import reactor.tcp.netty.NettyTcpClient;
import reactor.tcp.spec.TcpClientSpec;
import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Jon Brisbin
 */
public class TcpClientTests {

	private static final int ECHO_SERVER_PORT  = 27487;
	private static final int ABORT_SERVER_PORT = 27488;

	private final EchoServer            echoServer  = new EchoServer(ECHO_SERVER_PORT);
	private final ConnectionAbortServer abortServer = new ConnectionAbortServer(ABORT_SERVER_PORT);

	Environment env;

	@Before
	public void setup() {
		env = new Environment();
		threadPool.submit(echoServer);
		threadPool.submit(abortServer);
	}

	@After
	public void cleanup() throws InterruptedException, IOException {
		echoServer.close();
		abortServer.close();
		threadPool.shutdownNow();
		threadPool.awaitTermination(60, TimeUnit.SECONDS);
	}

	@Test
	public void testTcpClient() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);

		TcpClient<String, String> client = new TcpClientSpec<String, String>(NettyTcpClient.class)
				.env(env)
				.codec(StandardCodecs.STRING_CODEC)
				.connect("localhost", ECHO_SERVER_PORT)
				.get();

		client.open().consume(new Consumer<TcpConnection<String, String>>() {
			@Override
			public void accept(TcpConnection<String, String> conn) {
				conn.in().consume(new Consumer<String>() {
					@Override
					public void accept(String s) {
						latch.countDown();
					}
				});
				conn.out().accept("Hello World!");
			}
		});

		latch.await(30, TimeUnit.SECONDS);

		client.close();

		assertThat("latch was counted down", latch.getCount(), is(0L));
	}


	@Test
	public void testTcpClientWithInetSocketAddress() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);

		TcpClient<String, String> client = new TcpClientSpec<String, String>(NettyTcpClient.class)
				.env(env)
				.codec(StandardCodecs.STRING_CODEC)
				.connect(new InetSocketAddress("localhost", ECHO_SERVER_PORT))
				.get();

		client.open().consume(new Consumer<TcpConnection<String, String>>() {
			@Override
			public void accept(TcpConnection<String, String> conn) {
				conn.in().consume(new Consumer<String>() {
					@Override
					public void accept(String s) {
						latch.countDown();
					}
				});
				conn.out().accept("Hello World!");
			}
		});

		latch.await(30, TimeUnit.SECONDS);

		client.close();

		assertThat("latch was counted down", latch.getCount(), is(0L));
	}


	@Test
	public void tcpClientHandlesLineFeedData() throws InterruptedException {
		final int messages = 100;
		final CountDownLatch latch = new CountDownLatch(messages);
		final List<String> strings = new ArrayList<String>();

		TcpClient<String, String> client = new TcpClientSpec<String, String>(NettyTcpClient.class)
				.env(env)
				.codec(StandardCodecs.LINE_FEED_CODEC)
				.connect("localhost", ECHO_SERVER_PORT)
				.get();

		client.open().consume(new Consumer<TcpConnection<String, String>>() {
			@Override
			public void accept(TcpConnection<String, String> conn) {
				conn.in().consume(new Consumer<String>() {
					@Override
					public void accept(String s) {
						strings.add(s);
						latch.countDown();
					}
				});

				for (int i = 0; i < messages; i++) {
					conn.out().accept("Hello World!");
				}
			}
		});

		assertTrue("Expected messages not received. Received " + strings.size() + " messages: " + strings, latch.await(30, TimeUnit.SECONDS));
		client.close();

		assertEquals(messages, strings.size());
		Set<String> uniqueStrings = new HashSet<String>(strings);
		assertEquals(1, uniqueStrings.size());
		assertEquals("Hello World!", uniqueStrings.iterator().next());
	}

	@Test
	public void closingPromiseIsFulfilled() throws InterruptedException {
		TcpClient<String, String> client = new TcpClientSpec<String, String>(NettyTcpClient.class)
				.env(env)
				.codec(null)
				.connect("localhost", ECHO_SERVER_PORT)
				.get();

		final CountDownLatch closeLatch = new CountDownLatch(1);
		Promise<Void> p = client.close();
		p.onSuccess(new Consumer<Void>() {
			@Override
			public void accept(Void v) {
				closeLatch.countDown();
			}
		});
		// For some reason, the CI server want's to wait 30 seconds here, so we'll make sure
		// we wait long enough so the consumer actually has a chance to run.
		assertTrue("latch didn't time out", closeLatch.await(60, TimeUnit.SECONDS));
		assertThat("latch was counted down", closeLatch.getCount(), is(0L));
	}

	@Test
	public void smartReconnectWillReconnect() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicLong totalDelay = new AtomicLong();

		new TcpClientSpec<Buffer, Buffer>(NettyTcpClient.class)
				.env(env)
				.connect("localhost", ABORT_SERVER_PORT + 1)
				.get()
				.open(new Reconnect() {
					@Override
					public Tuple2<InetSocketAddress, Long> reconnect(InetSocketAddress currentAddress, int attempt) {
						switch (attempt) {
							case 1:
								totalDelay.addAndGet(100);
								return Tuple.of(currentAddress, 100L);
							case 2:
								totalDelay.addAndGet(500);
								return Tuple.of(currentAddress, 500L);
							case 3:
								totalDelay.addAndGet(1000);
								return Tuple.of(currentAddress, 1000L);
							default:
								latch.countDown();
								return null;
						}
					}
				});

		assertTrue("latch was counted down", latch.await(30, TimeUnit.SECONDS));
		assertThat("totalDelay was >1.6s", totalDelay.get(), greaterThanOrEqualTo(1600L));
	}

	private final ExecutorService threadPool = Executors.newCachedThreadPool();

	private static final class EchoServer implements Runnable {
		private volatile ServerSocketChannel server;
		private final    int                 port;

		private EchoServer(int port) {
			this.port = port;
		}

		@Override
		public void run() {
			try {
				server = ServerSocketChannel.open();
				server.socket().bind(new InetSocketAddress(port));
				server.configureBlocking(true);
				while (true) {
					SocketChannel ch = server.accept();

					ByteBuffer buffer = ByteBuffer.allocate(Buffer.SMALL_BUFFER_SIZE);
					while (true) {
						int read = ch.read(buffer);
						if (read > 0) {
							buffer.flip();
						}

						int written = ch.write(buffer);
						if (written < 0) {
							throw new IOException("Cannot write to client");
						}
						buffer.rewind();
					}
				}
			} catch (IOException e) {
				// Server closed
			}
		}

		public void close() throws IOException {
			ServerSocketChannel server = this.server;
			if (server != null) {
				server.close();
			}
		}
	}

	private static final class ConnectionAbortServer implements Runnable {
		final            int                 port;
		private volatile ServerSocketChannel server;

		private ConnectionAbortServer(int port) {
			this.port = port;
		}

		@Override
		public void run() {
			try {
				server = ServerSocketChannel.open();
				server.socket().bind(new InetSocketAddress(port));
				server.configureBlocking(true);
				while (true) {
					SocketChannel ch = server.accept();
					ch.close();
				}
			} catch (IOException e) {
				// Server closed
			}
		}

		public void close() throws IOException {
			ServerSocketChannel server = this.server;
			if (server != null) {
				server.close();
			}
		}
	}

}
