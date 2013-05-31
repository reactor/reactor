package reactor.tcp.syslog;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.string.StringDecoder;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Environment;
import reactor.fn.Consumer;
import reactor.tcp.TcpConnection;
import reactor.tcp.TcpServer;
import reactor.tcp.encoding.DelimitedCodec;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * @author Jon Brisbin
 */
public class SyslogTcpServerTests {

	static final byte[] SYSLOG_MESSAGE_DATA = "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8\n".getBytes();

	final int msgs    = 2000000;
	final int threads = 1;

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
	public void testSyslogServer() throws InterruptedException {
		EventLoopGroup bossGroup = new NioEventLoopGroup(1);
		EventLoopGroup workerGroup = new NioEventLoopGroup();

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
				 pipeline.addLast("syslogDecoder", new MessageToMessageDecoder<String, SyslogMessage>() {
					 @Override
					 public SyslogMessage decode(ChannelHandlerContext ctx, String msg) throws Exception {
						 return SyslogMessageParser.parse(msg);
					 }
				 });
				 pipeline.addLast("handler", new ChannelInboundMessageHandlerAdapter<SyslogMessage>() {
					 @Override
					 public void messageReceived(ChannelHandlerContext ctx, SyslogMessage msg) throws Exception {
						 latch.countDown();
					 }
				 });
			 }
		 });

		// Bind and start to accept incoming connections.
		b.bind().awaitUninterruptibly();

		for (int i = 0; i < threads; i++) {
			new SyslogMessageWriter().start();
		}

		latch.await(30, TimeUnit.SECONDS);
		end.set(System.currentTimeMillis());

		assertThat("latch was counted down", latch.getCount(), is(0L));

		double elapsed = (end.get() - start.get()) * 1.0;
		System.out.println("elapsed: " + (int) elapsed + "ms");
		System.out.println("throughput: " + (int) ((msgs * threads) / (elapsed / 1000)) + "/sec");
	}

	@Test
	public void testTcpSyslogServer() throws InterruptedException {
		TcpServer<Collection<SyslogMessage>, Collection<Void>> server = new TcpServer.Spec<Collection<SyslogMessage>, Collection<Void>>()
				.using(env)
				.sync()
				.codec(new DelimitedCodec<SyslogMessage, Void>(new SyslogCodec()))
				.consume(new Consumer<TcpConnection<Collection<SyslogMessage>, Collection<Void>>>() {
					@Override
					public void accept(TcpConnection<Collection<SyslogMessage>, Collection<Void>> conn) {
						conn.consume(new Consumer<Collection<SyslogMessage>>() {
							@Override
							public void accept(Collection<SyslogMessage> msgs) {
								count.addAndGet(msgs.size());
							}
						});
					}
				})
				.get()
				.start(
						new Consumer<Void>() {
							@Override
							public void accept(Void v) {
								for (int i = 0; i < threads; i++) {
									new SyslogMessageWriter().start();
								}
							}
						}
				);

		while (count.get() < (msgs * threads)) {
			end.set(System.currentTimeMillis());
			Thread.sleep(100);
		}

		double elapsed = (end.get() - start.get()) * 1.0;
		System.out.println("elapsed: " + (int) elapsed + "ms");
		System.out.println("throughput: " + (int) ((msgs * threads) / (elapsed / 1000)) + "/sec");

		server.shutdown();
	}

	private class SyslogMessageWriter extends Thread {
		@Override
		public void run() {
			try {
				java.nio.channels.SocketChannel ch = java.nio.channels.SocketChannel.open(new InetSocketAddress(3000));

				start.set(System.currentTimeMillis());
				for (int i = 0; i < msgs; i++) {
					ch.write(ByteBuffer.wrap(SYSLOG_MESSAGE_DATA));
				}
			} catch (IOException e) {
			}
		}
	}

}
