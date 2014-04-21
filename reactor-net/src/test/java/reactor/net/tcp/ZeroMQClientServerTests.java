package reactor.net.tcp;

import com.esotericsoftware.kryo.Kryo;
import org.junit.Test;
import reactor.io.Buffer;
import reactor.io.encoding.json.JacksonJsonCodec;
import reactor.io.encoding.kryo.KryoCodec;
import reactor.net.AbstractNetClientServerTest;
import reactor.net.zmq.tcp.ZeroMQ;
import reactor.net.zmq.tcp.ZeroMQTcpClient;
import reactor.net.zmq.tcp.ZeroMQTcpServer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * @author Jon Brisbin
 */
public class ZeroMQClientServerTests extends AbstractNetClientServerTest {

	Kryo                  kryo;
	KryoCodec<Data, Data> codec;
	CountDownLatch        latch;
	ZeroMQ<Data>          zmq;

	@Override
	public void setup() {
		super.setup();
		kryo = new Kryo();
		codec = new KryoCodec<>(kryo, false);
		latch = new CountDownLatch(1);
		zmq = new ZeroMQ<Data>(getServerEnvironment()).codec(codec);
	}

	@Test
	public void clientSendsDataToServerUsingKryo() throws InterruptedException {
		assertTcpClientServerExchangedData(ZeroMQTcpServer.class,
		                                   ZeroMQTcpClient.class,
		                                   codec,
		                                   data,
		                                   d -> d.equals(data));
	}

	@Test
	public void clientSendsDataToServerUsingJson() throws InterruptedException {
		assertTcpClientServerExchangedData(ZeroMQTcpServer.class,
		                                   ZeroMQTcpClient.class,
		                                   new JacksonJsonCodec<>(),
		                                   data,
		                                   d -> d.equals(data));
	}

	@Test
	public void clientSendsDataToServerUsingBuffers() throws InterruptedException {
		assertTcpClientServerExchangedData(ZeroMQTcpServer.class,
		                                   ZeroMQTcpClient.class,
		                                   Buffer.wrap("Hello World!"));
	}

	@Test
	public void zmqPushPull() throws InterruptedException {
		zmq.pull("tcp://*:" + getPort())
		   .consume(ch -> latch.countDown());

		Thread.sleep(500);

		zmq.push("tcp://localhost:" + getPort())
		   .consume(ch -> ch.send(data));

		assertTrue("PULL socket received data", latch.await(1, TimeUnit.SECONDS));

		zmq.shutdown();
	}

	@Test
	public void zmqRouterDealer() throws InterruptedException {
		zmq.router("tcp://*:" + getPort())
		   .consume(ch -> latch.countDown());

		Thread.sleep(500);

		zmq.dealer("tcp://localhost:" + getPort())
		   .consume(ch -> ch.send(data));

		assertTrue("ROUTER socket received data", latch.await(1, TimeUnit.SECONDS));

		zmq.shutdown();
	}

}
