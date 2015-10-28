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

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Publisher;
import reactor.Processors;
import reactor.Publishers;
import reactor.Subscribers;
import reactor.Timers;
import reactor.fn.Consumer;
import reactor.fn.tuple.Tuple;
import reactor.io.buffer.Buffer;
import reactor.io.net.NetStreams;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveNet;
import reactor.io.net.impl.netty.NettyBuffer;
import reactor.io.net.impl.netty.NettyClientSocketOptions;
import reactor.io.net.impl.netty.tcp.NettyTcpClient;
import reactor.io.net.preprocessor.CodecPreprocessor;
import reactor.io.net.tcp.support.SocketUtils;
import reactor.rx.Streams;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public class TcpClientTests {

	private final ExecutorService threadPool = Executors.newCachedThreadPool();
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
		Timers.global();

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
		threadPool.shutdownNow();
		threadPool.awaitTermination(5, TimeUnit.SECONDS);
		Thread.sleep(500);
	}

	@Test
	public void testTcpClient() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);

		TcpClient<String, String> client = ReactiveNet.tcpClient(s ->
			s.timer(Timers.global()).preprocessor(CodecPreprocessor.string()).connect("localhost", echoServerPort)
		);

		client.startAndAwait(conn -> {
			Streams.wrap(conn.input()).log("conn").consume(s -> {
				latch.countDown();
			});

			Streams.wrap(conn.writeWith(Streams.just("Hello World!"))).consume();

			return Streams.never();
		});

		latch.await(30, TimeUnit.SECONDS);

		client.shutdown();

		assertThat("latch was counted down", latch.getCount(), is(0L));
	}

	@Test
	public void testTcpClientWithInetSocketAddress() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);

		ReactorTcpClient<String, String> client = NetStreams.tcpClient(NettyTcpClient.class, spec -> spec
			.preprocessor(CodecPreprocessor.string())
			.connect(new InetSocketAddress(echoServerPort))
		);

		client.start(input -> {
			input.consume(d -> latch.countDown());
			input.writeWith(Streams.just("Hello")).consume();

			return Streams.never();
		}).await(5, TimeUnit.SECONDS);

		latch.await(5, TimeUnit.SECONDS);

		client.shutdown();

		assertThat("latch was counted down", latch.getCount(), is(0L));
	}

	@Test
	public void tcpClientHandlesLineFeedData() throws InterruptedException {
		final int messages = 100;
		final CountDownLatch latch = new CountDownLatch(messages);
		final List<String> strings = new ArrayList<String>();

		ReactorTcpClient<String, String> client = NetStreams.tcpClient(s ->
			s
			  .preprocessor(CodecPreprocessor.linefeed())
			  .connect("localhost", echoServerPort)
		);

		client.start(input -> {
			input.log("received").consume(s -> {
				strings.add(s);
				latch.countDown();
			});

			input.writeWith(
			  Streams.range(1, messages)
				.map(i -> "Hello World!")
				.publishOn(Processors.ioGroup("test-line-feed"))
			).consume();

			return Streams.never();
		}).await(5, TimeUnit.SECONDS);

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
		ReactorTcpClient<Buffer, Buffer> client = NetStreams.tcpClient(NettyTcpClient.class, spec -> spec
			.connect("localhost", abortServerPort)
		);

		client.start(null);

		assertTrue("Client was not closed within 30 seconds", client.shutdown().awaitSuccess(30, TimeUnit.SECONDS));
	}

	@Test
	public void connectionWillRetryConnectionAttemptWhenItFails() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicLong totalDelay = new AtomicLong();

		NetStreams.<Buffer, Buffer>tcpClient(s -> s
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

		latch.await(5, TimeUnit.SECONDS);
		assertTrue("latch was counted down:"+latch.getCount(), latch.getCount() == 0 );
		assertThat("totalDelay was >1.6s", totalDelay.get(), greaterThanOrEqualTo(1600L));
	}

	@Test
	public void connectionWillAttemptToReconnectWhenItIsDropped() throws InterruptedException, IOException {
		final CountDownLatch connectionLatch = new CountDownLatch(1);
		final CountDownLatch reconnectionLatch = new CountDownLatch(1);
		TcpClient<Buffer, Buffer> tcpClient = ReactiveNet.<Buffer, Buffer>tcpClient(s -> s
			.connect("localhost", abortServerPort)
		);

		tcpClient.start(connection -> {
			System.out.println("Start");
			connectionLatch.countDown();
			connection.input().subscribe(Subscribers.unbounded());
			return Publishers.never();
		}, (currentAddress, attempt) -> {
			reconnectionLatch.countDown();
			return null;
		}).subscribe(Subscribers.unbounded());

		assertTrue("Initial connection is made", connectionLatch.await(5, TimeUnit.SECONDS));
		assertTrue("A reconnect attempt was made", reconnectionLatch.await(5, TimeUnit.SECONDS));
	}

	@Test
	public void consumerSpecAssignsEventHandlers() throws InterruptedException, IOException {
		final CountDownLatch latch = new CountDownLatch(2);
		final CountDownLatch close = new CountDownLatch(1);
		final AtomicLong totalDelay = new AtomicLong();
		final long start = System.currentTimeMillis();

		TcpClient<Buffer, Buffer> client = ReactiveNet.<Buffer, Buffer>tcpClient(s -> s
			.connect("localhost", timeoutServerPort)
		);

		client.startAndAwait(p -> {
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

			  return Streams.timer(1).after().log();
		  }
		);

		assertTrue("latch was counted down", latch.await(5, TimeUnit.SECONDS));
		assertTrue("close was counted down", close.await(30, TimeUnit.SECONDS));
		assertThat("totalDelay was >500ms", totalDelay.get(), greaterThanOrEqualTo(500L));
	}

	@Test
	public void readIdleDoesNotFireWhileDataIsBeingRead() throws InterruptedException, IOException {
		final CountDownLatch latch = new CountDownLatch(1);
		long start = System.currentTimeMillis();

		TcpClient<Buffer, Buffer> client = ReactiveNet.tcpClient(s -> s
			.connect("localhost", heartbeatServerPort)
		);

		client.startAndAwait(p -> {
			  p.on()
				.readIdle(500, v -> {
					latch.countDown();
				});
			  return Streams.timer(1).after().log();
		  }
		);

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

		TcpClient<Buffer, Buffer> client = ReactiveNet.<Buffer, Buffer>tcpClient(s ->
			s.connect("localhost", echoServerPort)
		);

		client.startAndAwait(connection -> {
			System.out.println("hello");
			  connection.on()
				.writeIdle(500, v -> latch.countDown());

			  List<Publisher<Void>> allWrites = new ArrayList<>();
			  for (int i = 0; i < 5; i++) {
				  allWrites.add(connection.writeBufferWith(Streams.just(Buffer.wrap("a"))
				    .throttle(750)));
			  }
			  return Streams.merge(allWrites);
		  }
		);
		System.out.println("Started");

		assertTrue(latch.await(5, TimeUnit.SECONDS));

		long duration = System.currentTimeMillis() - start;

		assertThat(duration, is(greaterThanOrEqualTo(500l)));
	}

	@Test
	public void nettyNetChannelAcceptsNettyChannelHandlers() throws InterruptedException {
		TcpClient<Buffer, Buffer> client = ReactiveNet.tcpClient(NettyTcpClient.class,
		  spec -> spec
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
		client.startAndAwait(resp -> {
			latch.countDown();
			System.out.println("resp: " + resp);

			return resp.writeWith(Streams.just(NettyBuffer.create(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"))));
		});


		assertTrue("Latch didn't time out", latch.await(15, TimeUnit.SECONDS));
	}

	private static final class EchoServer implements Runnable {
		private final    int                 port;
		private final    ServerSocketChannel server;
		private volatile Thread              thread;

		private EchoServer(int port) {
			this.port = port;
			try {
				server = ServerSocketChannel.open();
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void run() {
			try {
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
		private final ServerSocketChannel server;

		private ConnectionAbortServer(int port) {
			this.port = port;
			try {
				server = ServerSocketChannel.open();
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void run() {
			try {
				server.socket().bind(new InetSocketAddress(port));
				server.configureBlocking(true);
				while (true) {
					SocketChannel ch = server.accept();
					System.out.println("ABORTING");
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
		private final ServerSocketChannel server;

		private ConnectionTimeoutServer(int port) {
			this.port = port;
			try {
				server = ServerSocketChannel.open();
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void run() {
			try {
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
		private final ServerSocketChannel server;

		private HeartbeatServer(int port) {
			this.port = port;
			try {
				server = ServerSocketChannel.open();
			}
			catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void run() {
			try {
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
