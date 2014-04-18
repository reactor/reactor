package reactor.net.tcp;

import com.esotericsoftware.kryo.Kryo;
import org.junit.Test;
import reactor.io.Buffer;
import reactor.io.encoding.json.JacksonJsonCodec;
import reactor.io.encoding.kryo.KryoCodec;
import reactor.net.AbstractNetClientServerTest;
import reactor.net.zmq.tcp.ZeroMQTcpClient;
import reactor.net.zmq.tcp.ZeroMQTcpServer;

/**
 * @author Jon Brisbin
 */
public class ZeroMQClientServerTests extends AbstractNetClientServerTest {

	@Test
	public void clientSendsDataToServerUsingKryo() throws InterruptedException {
		Kryo kryo = new Kryo();
		Data data = generateData();

		assertTcpClientServerExchangedData(ZeroMQTcpServer.class,
		                                   ZeroMQTcpClient.class,
		                                   new KryoCodec<>(kryo, false),
		                                   data,
		                                   d -> d.equals(data));
	}

	@Test
	public void clientSendsDataToServerUsingJson() throws InterruptedException {
		Data data = generateData();

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


}
