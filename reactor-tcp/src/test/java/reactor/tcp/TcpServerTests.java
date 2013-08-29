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

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LineBasedFrameDecoder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Environment;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.io.Buffer;
import reactor.tcp.config.ServerSocketOptions;
import reactor.tcp.config.SslOptions;
import reactor.tcp.encoding.Frame;
import reactor.tcp.encoding.FrameCodec;
import reactor.tcp.encoding.LengthFieldCodec;
import reactor.tcp.encoding.StandardCodecs;
import reactor.tcp.encoding.json.JsonCodec;
import reactor.tcp.netty.NettyServerSocketOptions;
import reactor.tcp.netty.NettyTcpClient;
import reactor.tcp.netty.NettyTcpServer;
import reactor.tcp.spec.TcpClientSpec;
import reactor.tcp.spec.TcpServerSpec;

import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author Jon Brisbin
 */
public class TcpServerTests {

	final ExecutorService threadPool = Executors.newCachedThreadPool();
	final AtomicInteger   port       = new AtomicInteger(24887);
	final int             msgs       = 100;
	final int             threads    = 2;

	Environment    env;
	CountDownLatch latch;
	AtomicLong count = new AtomicLong();
	AtomicLong start = new AtomicLong();
	AtomicLong end   = new AtomicLong();

	@Before
	public void loadEnv() {
		env = new Environment();
		latch = new CountDownLatch(msgs * threads);
	}

	@After
	public void cleanup() {
		threadPool.shutdownNow();
	}

	@Test
	public void tcpServerHandlesJsonPojosOverSsl() throws InterruptedException {
		final int port = this.port.incrementAndGet();
		SslOptions serverOpts = new SslOptions()
				//.keystoreFile("./src/test/resources/server.jks")
				.keystoreFile("reactor/tcp/server.jks")
				.keystorePasswd("changeit");

		SslOptions clientOpts = new SslOptions()
				//.keystoreFile("./src/test/resources/client.jks")
				.keystoreFile("reactor/tcp/client.jks")
				.keystorePasswd("changeit")
				.trustManagers(new Supplier<TrustManager[]>() {
					@Override
					public TrustManager[] get() {
						return new TrustManager[]{
								new X509TrustManager() {
									@Override
									public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
										// trust all
									}

									@Override
									public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
										// trust all
									}

									@Override
									public X509Certificate[] getAcceptedIssuers() {
										return new X509Certificate[0];
									}
								}
						};
					}
				});

		JsonCodec<Pojo, Pojo> codec = new JsonCodec<Pojo, Pojo>(Pojo.class);

		final CountDownLatch latch = new CountDownLatch(1);
		final TcpClient<Pojo, Pojo> client = new TcpClientSpec<Pojo, Pojo>(NettyTcpClient.class)
				.env(env)
				.ssl(clientOpts)
				.codec(codec)
				.connect("localhost", port)
				.get();

		TcpServer<Pojo, Pojo> server = new TcpServerSpec<Pojo, Pojo>(NettyTcpServer.class)
				.env(env)
				.ssl(serverOpts)
				.listen("localhost", port)
				.codec(codec)
				.consume(new Consumer<TcpConnection<Pojo, Pojo>>() {
					@Override
					public void accept(TcpConnection<Pojo, Pojo> conn) {
						conn.consume(new Consumer<Pojo>() {
							@Override
							public void accept(Pojo data) {
								if ("John Doe".equals(data.getName())) {
									latch.countDown();
								}
							}
						});
					}
				})
				.get()
				.start(new Consumer<Void>() {
					@Override
					public void accept(Void v) {
						try {
							client.open().await().send(new Pojo("John Doe"));
						} catch (InterruptedException e) {
							throw new IllegalStateException(e.getMessage(), e);
						}
					}
				});

		assertTrue("Latch was counted down", latch.await(5, TimeUnit.SECONDS));

		client.close().await();
		server.shutdown().await();
	}

	@Test
	public void tcpServerHandlesLengthFieldData() throws InterruptedException {
		final int port = this.port.incrementAndGet();

		TcpServer<byte[], byte[]> server = new TcpServerSpec<byte[], byte[]>(NettyTcpServer.class)
				.env(env)
				.synchronousDispatcher()
				.options(new ServerSocketOptions()
										 .backlog(1000)
										 .reuseAddr(true)
										 .tcpNoDelay(true))
				.listen(port)
				.codec(new LengthFieldCodec<byte[], byte[]>(StandardCodecs.BYTE_ARRAY_CODEC))
				.consume(new Consumer<TcpConnection<byte[], byte[]>>() {
					@Override
					public void accept(TcpConnection<byte[], byte[]> conn) {
						conn.consume(new Consumer<byte[]>() {
							long num = 1;

							@Override
							public void accept(byte[] bytes) {
								latch.countDown();
								ByteBuffer bb = ByteBuffer.wrap(bytes);
								if (bb.remaining() < 4) {
									System.err.println("insufficient len: " + bb.remaining());
								}
								int next = bb.getInt();
								if (next != num++) {
									System.err.println(this + " expecting: " + next + " but got: " + (num - 1));
								}
							}
						});
					}
				})
				.get()
				.start(new Consumer<Void>() {
					@Override
					public void accept(Void v) {
						start.set(System.currentTimeMillis());
						for (int i = 0; i < threads; i++) {
							threadPool.submit(new LengthFieldMessageWriter(port));
						}
					}
				});

		assertTrue("Latch was counted down", latch.await(5, TimeUnit.SECONDS));
		end.set(System.currentTimeMillis());

		double elapsed = (end.get() - start.get()) * 1.0;
		System.out.println("elapsed: " + (int) elapsed + "ms");
		System.out.println("throughput: " + (int) ((msgs * threads) / (elapsed / 1000)) + "/sec");

		server.shutdown().await();
	}

	@Test
	public void tcpServerHandlesFrameData() throws InterruptedException {
		final int port = this.port.incrementAndGet();

		TcpServer<Frame, Frame> server = new TcpServerSpec<Frame, Frame>(NettyTcpServer.class)
				.env(env)
						//.synchronousDispatcher()
				.dispatcher(Environment.RING_BUFFER)
				.options(new ServerSocketOptions()
										 .backlog(1000)
										 .reuseAddr(true)
										 .tcpNoDelay(true))
				.listen(port)
				.codec(new FrameCodec(2, FrameCodec.LengthField.SHORT))
				.consume(new Consumer<TcpConnection<Frame, Frame>>() {
					@Override
					public void accept(TcpConnection<Frame, Frame> conn) {
						conn.consume(new Consumer<Frame>() {
							@Override
							public void accept(Frame frame) {
								short prefix = frame.getPrefix().readShort();
								assertThat("prefix is 0", prefix == 0);
								Buffer data = frame.getData();
								assertThat("len is 128", data.remaining() == 128);

								latch.countDown();
							}
						});
					}
				})
				.get()
				.start(new Consumer<Void>() {
					@Override
					public void accept(Void v) {
						start.set(System.currentTimeMillis());
						for (int i = 0; i < threads; i++) {
							threadPool.submit(new FramedLengthFieldMessageWriter(port));
						}
					}
				});

		assertTrue("Latch was counted down", latch.await(5, TimeUnit.SECONDS));
		end.set(System.currentTimeMillis());

		double elapsed = (end.get() - start.get()) * 1.0;
		System.out.println("elapsed: " + (int) elapsed + "ms");
		System.out.println("throughput: " + (int) ((msgs * threads) / (elapsed / 1000)) + "/sec");

		server.shutdown().await();
	}

	@Test
	public void exposesNettyPipelineConfiguration() throws InterruptedException {
		final int port = this.port.incrementAndGet();
		final CountDownLatch latch = new CountDownLatch(2);

		final TcpClient<String, String> client = new TcpClientSpec<String, String>(NettyTcpClient.class)
				.env(env)
				.connect("localhost", port)
				.codec(StandardCodecs.LINE_FEED_CODEC)
				.get();

		Consumer<TcpConnection<String, String>> serverHandler = new Consumer<TcpConnection<String, String>>() {
			@Override
			public void accept(TcpConnection<String, String> conn) {
				conn.consume(new Consumer<String>() {
					@Override
					public void accept(String data) {
						latch.countDown();
					}
				});
			}
		};

		TcpServer<String, String> server = new TcpServerSpec<String, String>(NettyTcpServer.class)
				.env(env)
				.options(new NettyServerSocketOptions()
										 .pipelineConfigurer(new Consumer<ChannelPipeline>() {
											 @Override
											 public void accept(ChannelPipeline pipeline) {
												 pipeline.addLast(new LineBasedFrameDecoder(8 * 1024));
											 }
										 }))
				.listen("localhost", port)
				.codec(StandardCodecs.STRING_CODEC)
				.consume(serverHandler)
				.get()
				.start(new Consumer<Void>() {
					@Override
					public void accept(Void v) {
						client.open().onSuccess(new Consumer<TcpConnection<String, String>>() {
							@Override
							public void accept(TcpConnection<String, String> conn) {
								conn.send("Hello World!")
										.send("Hello World!");
							}
						});
					}
				});

		assertTrue("Latch was counted down", latch.await(5, TimeUnit.SECONDS));

		client.close().await();
		server.shutdown().await();
	}

	public static class Pojo {
		private String name;

		private Pojo() {
		}

		private Pojo(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return "Pojo{" +
					"name='" + name + '\'' +
					'}';
		}
	}

	private class LengthFieldMessageWriter extends Thread {
		private final Random rand = new Random();
		private final int port;
		private final int length;

		private LengthFieldMessageWriter(int port) {
			this.port = port;
			this.length = rand.nextInt(256);
		}

		@Override
		public void run() {
			try {
				java.nio.channels.SocketChannel ch = java.nio.channels.SocketChannel.open(new InetSocketAddress(port));

				System.out.println("writing " + msgs + " messages of " + length + " byte length...");

				int num = 1;
				start.set(System.currentTimeMillis());
				for (int j = 0; j < msgs; j++) {
					ByteBuffer buff = ByteBuffer.allocate(length + 4);
					buff.putInt(length);
					buff.putInt(num++);
					buff.position(0);
					buff.limit(length + 4);

					ch.write(buff);

					count.incrementAndGet();
				}
			} catch (IOException e) {
			}
		}
	}

	private class FramedLengthFieldMessageWriter extends Thread {
		private final short length = 128;
		private final int port;

		private FramedLengthFieldMessageWriter(int port) {
			this.port = port;
		}

		@Override
		public void run() {
			try {
				java.nio.channels.SocketChannel ch = java.nio.channels.SocketChannel.open(new InetSocketAddress(port));

				System.out.println("writing " + msgs + " messages of " + length + " byte length...");

				start.set(System.currentTimeMillis());
				for (int j = 0; j < msgs; j++) {
					ByteBuffer buff = ByteBuffer.allocate(length + 4);
					buff.putShort((short) 0);
					buff.putShort(length);
					for (int i = 4; i < length; i++) {
						buff.put((byte) 1);
					}
					buff.flip();
					buff.limit(length + 4);

					ch.write(buff);

					count.incrementAndGet();
				}
			} catch (IOException e) {
			}
		}
	}

}
