package reactor.spring.integration.syslog;

import com.eaio.uuid.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.Message;
import org.springframework.integration.MessageChannel;
import org.springframework.integration.MessageHeaders;
import org.springframework.integration.endpoint.MessageProducerSupport;
import reactor.core.Environment;
import reactor.tcp.TcpServer;
import reactor.tcp.encoding.syslog.SyslogCodec;
import reactor.tcp.encoding.syslog.SyslogConsumer;
import reactor.tcp.encoding.syslog.SyslogMessage;
import reactor.tcp.netty.NettyTcpServer;
import reactor.util.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Jon Brisbin
 */
public class SyslogInboundChannelAdapter extends MessageProducerSupport {

	private final Environment env;
	private volatile int    port       = 5140;
	private volatile String dispatcher = Environment.RING_BUFFER;
	private volatile MessageChannel                 outputChannel;
	private volatile TcpServer<SyslogMessage, Void> server;

	@Autowired
	public SyslogInboundChannelAdapter(Environment env) {
		this.env = env;
	}

	public void setPort(int port) {
		Assert.state(port > 0, "Port must be greater than 0");
		this.port = port;
	}

	public void setDispatcher(String dispatcher) {
		this.dispatcher = dispatcher;
	}

	public void setOutputChannel(MessageChannel outputChannel) {
		this.outputChannel = outputChannel;
		super.setOutputChannel(outputChannel);
	}

	@Override
	protected void doStart() {
		if (null != server) {
			throw new IllegalStateException("Server has already been started.");
		}
		this.server = new TcpServer.Spec<SyslogMessage, Void>(NettyTcpServer.class)
				.using(env)
				.listen(port)
				.dispatcher(dispatcher)
				.codec(new SyslogCodec())
				.consume(new SyslogConsumer() {
					@Override
					protected void accept(final SyslogMessage msg) {
						outputChannel.send(new Message<SyslogMessage>() {
							MessageHeaders headers;

							{
								UUID uuid = new UUID();
								Map<String, Object> headers = new HashMap<String, Object>();
								headers.put(MessageHeaders.ID, uuid);
							}

							@Override
							public MessageHeaders getHeaders() {
								return headers;
							}

							@Override
							public SyslogMessage getPayload() {
								return msg;
							}
						});
					}
				})
				.get()
				.start();
	}

}
