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
import org.reactivestreams.Publisher;
import reactor.Environment;
import reactor.fn.Consumer;
import reactor.fn.tuple.Tuple;
import reactor.io.buffer.Buffer;
import reactor.io.codec.StandardCodecs;
import reactor.io.net.NetStreams;
import reactor.io.net.impl.netty.NettyClientSocketOptions;
import reactor.io.net.impl.netty.tcp.NettyTcpClient;
import reactor.io.net.impl.zmq.tcp.ZeroMQTcpClient;
import reactor.io.net.impl.zmq.tcp.ZeroMQTcpServer;
import reactor.io.net.tcp.support.SocketUtils;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Streams;

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

		TcpClient<String, String> client = NetStreams.tcpClient(s ->
			s.env(env).codec(StandardCodecs.STRING_CODEC).connect("localhost", echoServerPort)
		);

		client.start(conn -> {
			conn.log("conn").consume(s -> {
				latch.countDown();
			});
			return conn.writeWith(Streams.just("Hello World!"));
		});

		latch.await(30, TimeUnit.SECONDS);

		client.shutdown();

		assertThat("latch was counted down", latch.getCount(), is(0L));
	}

	@Test
	public void testTcpClientWithInetSocketAddress() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);

		TcpClient<String, String> client = NetStreams.tcpClient(NettyTcpClient.class, spec -> spec
			.env(env)
			.codec(StandardCodecs.STRING_CODEC)
			.connect(new InetSocketAddress(echoServerPort))
		);

		client.start(input -> {
			input.consume(d -> latch.countDown());
			return input.writeWith(Streams.just("Hello"));
		});

		latch.await(30, TimeUnit.SECONDS);

		client.shutdown();

		assertThat("latch was counted down", latch.getCount(), is(0L));
	}

	@Test
	public void tcpClientHandlesLineFeedData() throws InterruptedException {
		final int messages = 100;
		final CountDownLatch latch = new CountDownLatch(messages);
		final List<String> strings = new ArrayList<String>();

		TcpClient<String, String> client = NetStreams.tcpClient(s ->
			s
			  .env(env)
			  .codec(StandardCodecs.LINE_FEED_CODEC)
			  .connect("localhost", echoServerPort)
		);

		client.start(input -> {
			input.log("received").consume(s -> {
				strings.add(s);
				latch.countDown();
			});

			return input.writeWith(
			  Streams.range(1, messages)
				.map(i -> "Hello World!")
				.subscribeOn(env.getDefaultDispatcher())
			);
		});

		assertTrue("Expected messages not received. Received " + strings.size() + " messages: " + strings,
		  latch.await(5, TimeUnit.SECONDS));
		client.shutdown();

		assertEquals(messages, strings.size());
		Set<String> uniqueStrings = new HashSet<String>(strings);
		assertEquals(1, uniqueStrings.size());
		assertEquals("Hello World!", uniqueStrings.iterator().next());
	}

	@Test
	public void closingPromiseIsFulfilled() throws InterruptedException {
		TcpClient<String, String> client = NetStreams.tcpClient(NettyTcpClient.class, spec -> spec
			.env(env)
			.codec(null)
			.connect("localhost", echoServerPort)
		);

		assertTrue("Client was not closed within 30 seconds", client.shutdown().awaitSuccess(30, TimeUnit.SECONDS));
	}

	@Test
	public void connectionWillRetryConnectionAttemptWhenItFails() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicLong totalDelay = new AtomicLong();

		NetStreams.<Buffer, Buffer>tcpClient(s -> s
			.env(env)
			.connect("localhost", abortServerPort + 3)
		)
		  .start(null, (currentAddress, attempt) -> {
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
		  }).consume(System.out::println);

		assertTrue("latch was counted down", latch.await(5, TimeUnit.SECONDS));
		assertThat("totalDelay was >1.6s", totalDelay.get(), greaterThanOrEqualTo(1600L));
	}

	@Test
	public void connectionWillAttemptToReconnectWhenItIsDropped() throws InterruptedException, IOException {
		final CountDownLatch connectionLatch = new CountDownLatch(1);
		final CountDownLatch reconnectionLatch = new CountDownLatch(1);
		TcpClient<Buffer, Buffer> tcpClient = NetStreams.<Buffer, Buffer>tcpClient(s -> s
			.env(env)
			.connect("localhost", abortServerPort)
		);

		tcpClient.start(connection -> {
			connectionLatch.countDown();
			connection.consume();
			return Streams.never();
		}, (currentAddress, attempt) -> {
			reconnectionLatch.countDown();
			return null;
		});

		assertTrue("Initial connection is made", connectionLatch.await(5, TimeUnit.SECONDS));
		assertTrue("A reconnect attempt was made", reconnectionLatch.await(5, TimeUnit.SECONDS));
	}

	@Test
	public void consumerSpecAssignsEventHandlers() throws InterruptedException, IOException {
		final CountDownLatch latch = new CountDownLatch(2);
		final CountDownLatch close = new CountDownLatch(1);
		final AtomicLong totalDelay = new AtomicLong();
		final long start = System.currentTimeMillis();

		TcpClient<Buffer, Buffer> client = NetStreams.<Buffer, Buffer>tcpClient(s -> s
			.env(env)
			.connect("localhost", timeoutServerPort)
		);

		client.start(p -> {
			  p.on()
				.close(v -> close.countDown())
				.readIdle(500, v -> {
					totalDelay.addAndGet(System.currentTimeMillis() - start);
					latch.countDown();
				})
				.writeIdle(500, v -> {
					totalDelay.addAndGet(System.currentTimeMillis() - start);
					latch.countDown();
				});

			  return Streams.timer(env.getTimer(), 1).after().log();
		  }
		).await(5, TimeUnit.SECONDS);

		assertTrue("latch was counted down", latch.await(5, TimeUnit.SECONDS));
		assertTrue("close was counted down", close.await(30, TimeUnit.SECONDS));
		assertThat("totalDelay was >500ms", totalDelay.get(), greaterThanOrEqualTo(500L));
	}

	@Test
	public void readIdleDoesNotFireWhileDataIsBeingRead() throws InterruptedException, IOException {
		final CountDownLatch latch = new CountDownLatch(1);
		long start = System.currentTimeMillis();

		TcpClient<Buffer, Buffer> client = NetStreams.tcpClient(s -> s
			.env(env)
			.connect("localhost", heartbeatServerPort)
		);

		client.start(p -> {
			  p.on()
				.readIdle(500, v -> {
					latch.countDown();
				});
			  return Streams.timer(env.getTimer(), 1).after().log();
		  }
		).await();

		Thread.sleep(700);
		heartbeatServer.close();

		assertTrue(latch.await(5, TimeUnit.SECONDS));

		long duration = System.currentTimeMillis() - start;

		assertThat(duration, is(greaterThanOrEqualTo(500L)));
	}

	@Test
	public void writeIdleDoesNotFireWhileDataIsBeingSent() throws InterruptedException, IOException {
		final CountDownLatch latch = new CountDownLatch(1);
		long start = System.currentTimeMillis();

		TcpClient<Buffer, Buffer> client = NetStreams.<Buffer, Buffer>tcpClient(s ->
			s.env(env)
			  .connect("localhost", echoServerPort)
		);


		client.start(connection -> {
			  connection.on()
				.writeIdle(500, v -> {
					latch.countDown();
				});

			  List<Publisher<Void>> allWrites = new ArrayList<>();
			  for (int i = 0; i < 5; i++) {
				  allWrites.add(connection.writeBufferWith(Streams.just(Buffer.wrap("a")).throttle(500, env.getTimer()
				  )));
			  }
			  return Streams.merge(allWrites);
		  }
		).await();

		assertTrue(latch.await(5, TimeUnit.SECONDS));

		long duration = System.currentTimeMillis() - start;

		assertThat(duration, is(greaterThanOrEqualTo(500l)));
	}

	@Test
	public void nettyNetChannelAcceptsNettyChannelHandlers() throws InterruptedException {
		TcpClient<HttpObject, HttpRequest> client = NetStreams.<HttpObject, HttpRequest>tcpClient(NettyTcpClient.class,
		  spec -> spec
			.env(env)
			.options(new NettyClientSocketOptions()
			  .pipelineConfigurer(new Consumer<ChannelPipeline>() {
				  @Override
				  public void accept(ChannelPipeline pipeline) {
					  pipeline.addLast(new HttpClientCodec());
				  }
			  }))
			.connect("www.google.com", 80)
		);


		final CountDownLatch latch = new CountDownLatch(1);
		client.start(resp -> {
			latch.countDown();
			System.out.println("resp: " + resp);

			return resp.writeWith(Streams.just(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")));
		}).await();


		assertTrue("Latch didn't time out", latch.await(15, TimeUnit.SECONDS));
	}

	@Test
	public void zmqClientServerInteraction() throws InterruptedException {
		final int port = SocketUtils.findAvailableTcpPort();
		final CountDownLatch latch = new CountDownLatch(2);

		TcpServer<Buffer, Buffer> zmqs = NetStreams.tcpServer(ZeroMQTcpServer.class, spec -> spec
			.env(env)
			.listen(port)
		);

		zmqs.start(ch ->
			ch.writeWith(ch.log("zmq").take(1).map(buff -> {
				if (buff.remaining() == 12) {
					latch.countDown();
				}
				return Buffer.wrap("Goodbye World!");
			}))
		).await(5, TimeUnit.SECONDS);

		TcpClient<Buffer, Buffer> zmqc = NetStreams.<Buffer, Buffer>tcpClient(ZeroMQTcpClient.class, s -> s
			.env(env)
			.connect("127.0.0.1", port)
		);

		final Promise<Buffer> promise = Promises.prepare();

		zmqc.start(ch -> {
			ch.log("zmq-c").subscribe(promise);
			return ch.writeWith(Streams.just(Buffer.wrap("Hello World!")));
		}).await(5, TimeUnit.SECONDS);

		String msg = promise
		  .await(30, TimeUnit.SECONDS)
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
