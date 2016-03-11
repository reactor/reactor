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

package reactor.io.netty.tcp.syslog;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.string.StringDecoder;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import reactor.Environment;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.netty.NetStreams;
import reactor.io.netty.codec.syslog.SyslogCodec;
import reactor.io.netty.codec.syslog.SyslogMessage;
import reactor.io.netty.tcp.TcpServer;
import reactor.io.netty.tcp.syslog.hdfs.HdfsConsumer;
import reactor.rx.Streams;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class SyslogTcpServerTests {

	static final byte[] SYSLOG_MESSAGE_DATA = ("<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on " +
			"/dev/pts/8\n")
			.getBytes();

	final int msgs    = 2000000;
	final int threads = 4;

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

	@Test
	@Ignore
	public void testSyslogServer() throws InterruptedException, IOException {
		EventLoopGroup bossGroup = new NioEventLoopGroup(2);
		EventLoopGroup workerGroup = new NioEventLoopGroup(4);

		Configuration conf = new Configuration();
		conf.addResource("/usr/local/Cellar/hadoop/1.1.2/libexec/conf/core-site.xml");
		final HdfsConsumer hdfs = new HdfsConsumer(conf, "loadtests", "syslog");

		ServerBootstrap b = new ServerBootstrap();
		b.group(bossGroup, workerGroup)
				.localAddress(3000)
				.channel(NioServerSocketChannel.class)
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel ch) throws Exception {
						ChannelPipeline pipeline = ch.pipeline();
						pipeline.addLast("framer", new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter()));
						pipeline.addLast("decoder", new StringDecoder());
						pipeline.addLast("syslogDecoder", new MessageToMessageDecoder<String>() {
							Function<Buffer, SyslogMessage> decoder = new SyslogCodec().decoder(null);

							@Override
							public void decode(ChannelHandlerContext ctx, String msg, List<Object> messages) throws Exception {
								messages.add(decoder.apply(Buffer.wrap(msg + "\n")));
							}
						});
						pipeline.addLast("handler", new ChannelInboundHandlerAdapter() {

							@Override
							public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
								latch.countDown();
								hdfs.accept((SyslogMessage) msg);
							}
						});
					}
				});

		// Bind and start to accept incoming connections.
		ChannelFuture channelFuture = b.bind().awaitUninterruptibly();

		for (int i = 0; i < threads; i++) {
			new SyslogMessageWriter(3000).start();
		}

		latch.await(60, TimeUnit.SECONDS);
		end.set(System.currentTimeMillis());

		assertThat("latch was counted down", latch.getCount(), is(0L));

		double elapsed = (end.get() - start.get()) * 1.0;
		System.out.println("elapsed: " + (int) elapsed + "ms");
		System.out.println("throughput: " + (int) ((msgs * threads) / (elapsed / 1000)) + "/sec");

		channelFuture.channel().close().awaitUninterruptibly();
	}

	@Test
	@Ignore
	public void testTcpSyslogServer() throws InterruptedException, IOException {
		//final FileChannelConsumer<SyslogMessage> fcc = new FileChannelConsumer<SyslogMessage>(".", "syslog", -1, -1);
		Configuration conf = new Configuration();
		conf.addResource("/usr/local/Cellar/hadoop/1.1.2/libexec/conf/core-site.xml");
		final HdfsConsumer hdfs = new HdfsConsumer(conf, "loadtests", "syslog");

		TcpServer<SyslogMessage, Void> server = NetStreams.tcpServer(spec ->
						spec
								.env(env)
										//.using(SynchronousDispatcher.INSTANCE)
										//.dispatcher(Environment.DISPATCHER_GROUP)
								.dispatcher(Environment.SHARED)
								.codec(new SyslogCodec())
		);

		server.start(conn -> {
					conn.consume(msg -> {
						count.incrementAndGet();
					});

					conn.consume(hdfs);
					return Streams.never();
				}
		).await();

		for (int i = 0; i < threads; i++) {
			new SyslogMessageWriter(server.getListenAddress().getPort()).start();
		}

		while (count.get() < (msgs * threads)) {
			end.set(System.currentTimeMillis());
			Thread.sleep(100);
		}

		double elapsed = (end.get() - start.get()) * 1.0;
		System.out.println("elapsed: " + (int) elapsed + "ms");
		System.out.println("throughput: " + (int) ((msgs * threads) / (elapsed / 1000)) + "/sec");

		server.shutdown();
	}

	@Test
	@Ignore
	public void testExternalServer() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);

		long start = System.currentTimeMillis();
		SyslogMessageWriter[] writers = new SyslogMessageWriter[threads];
		for (int i = 0; i < threads; i++) {
			writers[i] = new SyslogMessageWriter(5140);
			writers[i].start();
		}

		latch.await(10, TimeUnit.SECONDS);
		// Calculate exact time, which will be slightly over timeout
		long end = System.currentTimeMillis();
		double elapsed = (end - start) * 1.0;

		int totalMsgs = 0;
		for (int i = 0; i < threads; i++) {
			totalMsgs += writers[i].count.intValue();
		}

		System.out.println("throughput: " + (int) ((totalMsgs) / (elapsed / 1000)) + "/sec");
	}

	private class SyslogMessageWriter extends Thread {
		AtomicLong count = new AtomicLong();
		private final int port;

		private SyslogMessageWriter(int port) {
			this.port = port;
		}

		@Override
		public void run() {
			try {
				java.nio.channels.SocketChannel ch = java.nio.channels.SocketChannel.open(new InetSocketAddress(port));

				start.set(System.currentTimeMillis());
				for (int i = 0; i < msgs; i++) {
					ch.write(ByteBuffer.wrap(SYSLOG_MESSAGE_DATA));
					count.incrementAndGet();
				}
			} catch (IOException e) {
			}
		}
	}

}
