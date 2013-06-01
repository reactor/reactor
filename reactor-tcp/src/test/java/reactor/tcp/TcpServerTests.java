package reactor.tcp;

import org.junit.Before;
import org.junit.Test;
import reactor.core.Environment;
import reactor.fn.Consumer;
import reactor.tcp.encoding.StandardCodecs;
import reactor.tcp.netty.NettyTcpServer;

import java.util.Collection;

/**
 * @author Jon Brisbin
 */
public class TcpServerTests {

	Environment env;

	@Before
	public void loadEnv() {
		env = new Environment();
	}

	@Test
	public void testTcpServer() throws InterruptedException {
		TcpServer<Collection<String>, Collection<String>> server = new TcpServer.Spec<Collection<String>, Collection<String>>(NettyTcpServer.class)
				.using(env)
				.ringBuffer()
				.codec(StandardCodecs.LINE_FEED_CODEC)
				.consume(new Consumer<TcpConnection<Collection<String>, Collection<String>>>() {
					@Override
					public void accept(TcpConnection<Collection<String>, Collection<String>> conn) {
						conn.consume(new Consumer<Collection<String>>() {
							@Override
							public void accept(Collection<String> data) {
								System.out.println("got data: " + data);
							}
						});
					}
				})
				.get()
				.start();

		while (true) {
			Thread.sleep(5000);
		}
	}

}
