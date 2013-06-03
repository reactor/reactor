package reactor.tcp;

import org.junit.Before;
import org.junit.Test;
import reactor.core.Environment;
import reactor.fn.Consumer;
import reactor.tcp.encoding.json.JsonCodec;
import reactor.tcp.netty.NettyTcpServer;

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
		TcpServer<Pojo, Pojo> server = new TcpServer.Spec<Pojo, Pojo>(NettyTcpServer.class)
				.using(env)
				.ringBuffer()
				.codec(new JsonCodec<Pojo, Pojo>(Pojo.class, Pojo.class))
				.consume(new Consumer<TcpConnection<Pojo, Pojo>>() {
					@Override
					public void accept(TcpConnection<Pojo, Pojo> conn) {
						conn.consume(new Consumer<Pojo>() {
							@Override
							public void accept(Pojo data) {
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

	private static class Pojo {
		String name;

		private Pojo() {
		}

		private Pojo(String name) {
			this.name = name;
		}

		private String getName() {
			return name;
		}

		private void setName(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return "Pojo{" +
					"name='" + name + '\'' +
					'}';
		}
	}

}
