package reactor.tcp;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Environment;
import reactor.fn.Consumer;
import reactor.io.Buffer;
import reactor.tcp.encoding.StandardCodecs;
import reactor.tcp.netty.NettyTcpClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * @author Jon Brisbin
 */
public class TcpClientTests {

	static final int port = 24887;

	Environment env;

	@Before
	public void setup() {
		env = new Environment();
		threadPool.submit(new EchoServer());
	}

	@After
	public void cleanup() {
		threadPool.shutdown();
	}

	@Test
	public void testTcpClient() throws InterruptedException {
		final CountDownLatch latch = new CountDownLatch(1);

		TcpClient<String, String> client = new TcpClient.Spec<String, String>(NettyTcpClient.class)
				.using(env)
				.codec(StandardCodecs.STRING_CODEC)
				.connect("localhost", port)
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

	private final ExecutorService threadPool = Executors.newCachedThreadPool();

	private static final class EchoServer implements Runnable {
		@Override
		public void run() {
			try {
				ServerSocketChannel server = ServerSocketChannel.open();
				server.bind(new InetSocketAddress(port));
				server.configureBlocking(true);
				while (true) {
					SocketChannel ch = server.accept();

					ByteBuffer buffer = ByteBuffer.allocate(Buffer.SMALL_BUFFER_SIZE);
					int read = ch.read(buffer);
					if (read > 0) {
						buffer.flip();
					}

					int written = ch.write(buffer);
					if (written < 0) {
						throw new IOException("Cannot write to client");
					}

					ch.close();
				}
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}

		}
	}

}
