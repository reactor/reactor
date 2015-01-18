/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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

package reactor.io.net.tcp;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.Environment;
import reactor.fn.Consumer;
import reactor.fn.batch.BatchConsumer;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.io.buffer.Buffer;
import reactor.io.codec.StandardCodecs;
import reactor.io.net.NetChannelStream;
import reactor.io.net.NetClient;
import reactor.io.net.Reconnect;
import reactor.io.net.netty.NettyClientSocketOptions;
import reactor.io.net.netty.tcp.NettyTcpClient;
import reactor.io.net.tcp.spec.TcpClientSpec;
import reactor.io.net.tcp.spec.TcpServerSpec;
import reactor.io.net.tcp.support.SocketUtils;
import reactor.io.net.zmq.tcp.ZeroMQTcpClient;
import reactor.io.net.zmq.tcp.ZeroMQTcpServer;

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
import static org.junit.Assert.*;

/**
 * @author Jon Brisbin
 */
public class TcpClientTests {

	private final ExecutorService threadPool = Executors.newCachedThreadPool();
	Environment             env;
	int                     echoServerPort;
	EchoServer              echoServer;
	int                     abortServerPort;
	ConnectionAbortServer   abortServer;
	int                     timeoutServerPort;
	ConnectionTimeoutServer timeoutServer;
	int                     heartbeatServerPort;
	HeartbeatServer         heartbeatServer;

	@Before
	public void setup() {
		env = new Environment();

		echoServerPort = SocketUtils.findAvailableTcpPort();
		echoServer = new EchoServer(echoServerPort);
		threadPool.submit(echoServer);

		abortServerPort = SocketUtils.findAvailableTcpPort();
		abortServer = new ConnectionAbortServer(abortServerPort);
		threadPool.submit(abortServer);

		timeoutServerPort = SocketUtils.findAvailableTcpPort();
		timeoutServer = new ConnectionTimeoutServer(timeoutServerPort);
		threadPool.submit(timeoutServer);

		heartbeatServerPort = SocketUtils.findAvailableTcpPort();
		heartbeatServer = new HeartbeatServer(heartbeatServerPort);
		threadPool.submit(heartbeatServer);
	}

	@After
	public void cleanup() throws InterruptedException, IOException {
		echoServer.close();
		abortServer.close();
		timeoutServer.close();
		heartbeatServer.close();
		threadPool.shutdown();
		threadPool.awaitTermination(5, TimeUnit.SECONDS);
		Thread.sleep(500);
	}

	@Test
	public void testTcpClient() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);

		TcpClient<String, String> client = new TcpClientSpec<String, String>(NettyTcpClient.class)
				.env(env)
				.codec(StandardCodecs.STRING_CODEC)
				.connect("localhost", echoServerPort)
				.get();

		client.open().onSuccess(new Consumer<NetChannelStream<String, String>>() {
			@Override
			public void accept(NetChannelStream<String, String> conn) {
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
				.connect(new InetSocketAddress(echoServerPort))
				.get();

		client.open().onSuccess(new Consumer<NetChannelStream<String, String>>() {
			@Override
			public void accept(NetChannelStream<String, String> conn) {
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
				.connect("localhost", echoServerPort)
				.get();

		client.open().onSuccess(new Consumer<NetChannelStream<String, String>>() {
			@Override
			public void accept(NetChannelStream<String, String> conn) {
				conn.in().consume(new Consumer<String>() {
					@Override
					public void accept(String s) {
						strings.add(s);
						latch.countDown();
					}
				});

				BatchConsumer<String> out = conn.out();
				out.start();
				for (int i = 0; i < messages; i++) {
					out.accept("Hello World!");
				}
				out.end();
			}
		});

		assertTrue("Expected messages not received. Received " + strings.size() + " messages: " + strings,
		           latch.await(5, TimeUnit.SECONDS));
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
				.connect("localhost", echoServerPort)
				.get();

		assertTrue("Client was not closed within 30 seconds", client.close().awaitSuccess(30, TimeUnit.SECONDS));
	}

	@Test
	public void connectionWillRetryConnectionAttemptWhenItFails() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicLong totalDelay = new AtomicLong();

		new TcpClientSpec<Buffer, Buffer>(NettyTcpClient.class)
				.env(env)
				.connect("localhost", abortServerPort + 3)
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

		assertTrue("latch was counted down", latch.await(5, TimeUnit.SECONDS));
		assertThat("totalDelay was >1.6s", totalDelay.get(), greaterThanOrEqualTo(1600L));
	}

	@Test
	public void connectionWillAttemptToReconnectWhenItIsDropped() throws InterruptedException, IOException {
		final CountDownLatch connectionLatch = new CountDownLatch(1);
		final CountDownLatch reconnectionLatch = new CountDownLatch(1);
		new TcpClientSpec<Buffer, Buffer>(NettyTcpClient.class)
				.env(env)
				.connect("localhost", abortServerPort)
				.get()
				.open(new Reconnect() {
					@Override
					public Tuple2<InetSocketAddress, Long> reconnect(InetSocketAddress currentAddress, int attempt) {
						reconnectionLatch.countDown();
						return null;
					}
				})
				.consume(new Consumer<NetChannelStream<Buffer, Buffer>>() {
					@Override
					public void accept(NetChannelStream<Buffer, Buffer> connection) {
						connectionLatch.countDown();
					}
				});

		assertTrue("Initial connection is made", connectionLatch.await(5, TimeUnit.SECONDS));
		assertTrue("A reconnect attempt was made", reconnectionLatch.await(5, TimeUnit.SECONDS));
	}

	@Test
	public void consumerSpecAssignsEventHandlers() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(3);
		final AtomicLong totalDelay = new AtomicLong();
		final long start = System.currentTimeMillis();

		new TcpClientSpec<Buffer, Buffer>(NettyTcpClient.class)
				.env(env)
				.connect("localhost", timeoutServerPort)
				.get().open().await(5, TimeUnit.SECONDS).on()
				.close( v-> latch.countDown() )
				.readIdle(500, v -> {
						totalDelay.addAndGet(System.currentTimeMillis() - start);
						latch.countDown();
				})
				.writeIdle(500, v -> {
						totalDelay.addAndGet(System.currentTimeMillis() - start);
						latch.countDown();
				});

		assertTrue("latch was counted down", latch.await(5, TimeUnit.SECONDS));
		assertThat("totalDelay was >500ms", totalDelay.get(), greaterThanOrEqualTo(500L));
	}

	@Test
	public void readIdleDoesNotFireWhileDataIsBeingRead() throws InterruptedException, IOException {
		final CountDownLatch latch = new CountDownLatch(1);
		long start = System.currentTimeMillis();

		new TcpClientSpec<Buffer, Buffer>(NettyTcpClient.class)
				.env(env)
				.connect("localhost", heartbeatServerPort)
				.get().open().await().on()
				.readIdle(500, v -> {
						latch.countDown();
				});

		Thread.sleep(700);
		heartbeatServer.close();

		assertTrue(latch.await(5, TimeUnit.SECONDS));

		long duration = System.currentTimeMillis() - start;

		assertThat(duration, is(greaterThanOrEqualTo(1000L)));
	}

	@Test
	public void writeIdleDoesNotFireWhileDataIsBeingSent() throws InterruptedException, IOException {
		final CountDownLatch latch = new CountDownLatch(1);
		long start = System.currentTimeMillis();

		NetChannelStream<Buffer, Buffer> connection = new TcpClientSpec<Buffer, Buffer>(NettyTcpClient.class)
				.env(env)
				.connect("localhost", echoServerPort)
				.get().open().await();

		connection.on()
		          .writeIdle(500, v ->  {
				          latch.countDown();
		          });

		for (int i = 0; i < 5; i++) {
			Thread.sleep(100);
			connection.echo(Buffer.wrap("a"));
		}

		assertTrue(latch.await(5, TimeUnit.SECONDS));

		long duration = System.currentTimeMillis() - start;

		assertThat(duration, is(greaterThanOrEqualTo(1000L)));
	}

	@Test
	public void nettyNetChannelAcceptsNettyChannelHandlers() throws InterruptedException {
		NetChannelStream<HttpObject, HttpRequest> connection =
				new TcpClientSpec<HttpObject, HttpRequest>(NettyTcpClient.class)
						.env(env)
						.options(new NettyClientSocketOptions()
								         .pipelineConfigurer(new Consumer<ChannelPipeline>() {
									         @Override
									         public void accept(ChannelPipeline pipeline) {
										         pipeline.addLast(new HttpClientCodec());
									         }
								         }))
						.connect("www.google.com", 80)
						.get().open().await();

		final CountDownLatch latch = new CountDownLatch(1);
		connection.sendAndReceive(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"))
		          .onSuccess(new Consumer<HttpObject>() {
			          @Override
			          public void accept(HttpObject resp) {
				          latch.countDown();
				          System.out.println("resp: " + resp);
			          }
		          });

		assertTrue("Latch didn't time out", latch.await(15, TimeUnit.SECONDS));
	}

	@Test
	public void zmqClientServerInteraction() throws InterruptedException {
		final int port = SocketUtils.findAvailableTcpPort();
		final CountDownLatch latch = new CountDownLatch(2);

		TcpServer<Buffer, Buffer> zmqs = new TcpServerSpec<Buffer, Buffer>(ZeroMQTcpServer.class)
				.env(env)
				.listen(port)
				.get();

		zmqs.consume(ch -> {
			ch.consume(buff -> {
				if (buff.remaining() == 12) {
					latch.countDown();
					ch.echo(Buffer.wrap("Goodbye World!"));
				}
			});
		});

		assertTrue("server was started", zmqs.start().awaitSuccess(5, TimeUnit.SECONDS));

		NetClient<Buffer, Buffer, ?> zmqc = new TcpClientSpec<Buffer, Buffer>(ZeroMQTcpClient.class)
				.env(env)
				.connect("127.0.0.1", port)
				.get();

		NetChannelStream<Buffer, Buffer> ch = zmqc.open().await(5, TimeUnit.SECONDS);
		assertNotNull("channel was connected", ch);


		String msg = ch.sendAndReceive(Buffer.wrap("Hello World!"))
		               .await(5, TimeUnit.SECONDS)
		               .asString();

		assertThat("messages were exchanged", msg, is("Goodbye World!"));
	}

	private static final class EchoServer implements Runnable {
		private final    int                 port;
		private volatile ServerSocketChannel server;
		private volatile Thread              thread;

		private EchoServer(int port) {
			this.port = port;
		}

		@Override
		public void run() {
			try {
				server = ServerSocketChannel.open();
				server.socket().bind(new InetSocketAddress(port));
				server.configureBlocking(true);
				thread = Thread.currentThread();
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
			Thread thread = this.thread;
			if (thread != null) {
				thread.interrupt();
			}
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
			} catch (Exception e) {
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

	private static final class ConnectionTimeoutServer implements Runnable {
		final            int                 port;
		private volatile ServerSocketChannel server;

		private ConnectionTimeoutServer(int port) {
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
					ByteBuffer buff = ByteBuffer.allocate(1);
					ch.read(buff);
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

	private static final class HeartbeatServer implements Runnable {
		final            int                 port;
		private volatile ServerSocketChannel server;

		private HeartbeatServer(int port) {
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
					while (server.isOpen()) {
						ByteBuffer out = ByteBuffer.allocate(1);
						out.put((byte) '\n');
						out.flip();
						ch.write(out);
						Thread.sleep(100);
					}
				}
			} catch (IOException e) {
				// Server closed
			} catch (InterruptedException ie) {

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
