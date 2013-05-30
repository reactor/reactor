package reactor.tcp;

import org.junit.Before;
import org.junit.Test;
import reactor.core.Environment;
import reactor.fn.Consumer;
import reactor.tcp.encoding.StandardCodecs;

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
		TcpServer<String, String> server = new TcpServer.Spec<String, String>()
				.using(env)
				.ringBuffer()
				.codec(StandardCodecs.LINE_FEED_CODEC)
				.consume(new Consumer<TcpConnection<String, String>>() {
					@Override
					public void accept(TcpConnection<String, String> conn) {
						conn.consume(new Consumer<String>() {
							@Override
							public void accept(String s) {
								System.out.println("got data: " + s);
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
