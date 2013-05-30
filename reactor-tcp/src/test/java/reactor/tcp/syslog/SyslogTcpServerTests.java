package reactor.tcp.syslog;

import org.junit.Before;
import org.junit.Test;
import reactor.core.Environment;
import reactor.fn.Consumer;
import reactor.tcp.TcpConnection;
import reactor.tcp.TcpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jon Brisbin
 */
public class SyslogTcpServerTests {

	static final byte[] SYSLOG_MESSAGE_DATA = "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8\n".getBytes();

	final int msgs    = 40000000;
	final int threads = 4;

	Environment    env;
	CountDownLatch latch;
	AtomicLong start = new AtomicLong();
	AtomicLong end   = new AtomicLong();

	@Before
	public void loadEnv() {
		env = new Environment();
		latch = new CountDownLatch(msgs * threads);
	}

	@Test
	public void testSyslogServer() throws InterruptedException {
		SyslogServer server = new SyslogServer.Spec()
				.using(env)
				.sync()
				.consume(new Consumer<TcpConnection<SyslogMessage, SyslogMessage>>() {
					@Override
					public void accept(TcpConnection<SyslogMessage, SyslogMessage> conn) {
						conn.consume(new Consumer<SyslogMessage>() {
							@Override
							public void accept(SyslogMessage msg) {
								latch.countDown();
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

		latch.await(30, TimeUnit.SECONDS);
		end.set(System.currentTimeMillis());

		double elapsed = (end.get() - start.get()) * 1.0;
		System.out.println("elapsed: " + (int) elapsed + "ms");
		System.out.println("throughput: " + (int) ((msgs * threads) / (elapsed / 1000)) + "/sec");

		server.shutdown();
	}

	@Test
	public void testTcpSyslogServer() throws InterruptedException {
		TcpServer<SyslogMessage, Void> server = new TcpServer.Spec<SyslogMessage, Void>()
				.using(env)
				.sync()
				.codec(new SyslogCodec())
				.consume(new Consumer<TcpConnection<SyslogMessage, Void>>() {
					@Override
					public void accept(TcpConnection<SyslogMessage, Void> conn) {
						conn.consume(new Consumer<SyslogMessage>() {
							@Override
							public void accept(SyslogMessage msg) {
								latch.countDown();
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

		latch.await(30, TimeUnit.SECONDS);
		end.set(System.currentTimeMillis());

		double elapsed = (end.get() - start.get()) * 1.0;
		System.out.println("elapsed: " + (int) elapsed + "ms");
		System.out.println("throughput: " + (int) ((msgs * threads) / (elapsed / 1000)) + "/sec");

		server.shutdown();
	}

	private class SyslogMessageWriter extends Thread {
		@Override
		public void run() {
			try {
				SocketChannel ch = SocketChannel.open(new InetSocketAddress(3000));

				start.set(System.currentTimeMillis());
				for (int i = 0; i < msgs; i++) {
					ch.write(ByteBuffer.wrap(SYSLOG_MESSAGE_DATA));
				}
			} catch (IOException e) {
			}
		}
	}

}
