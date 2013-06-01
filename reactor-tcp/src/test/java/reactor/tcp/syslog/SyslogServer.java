package reactor.tcp.syslog;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundMessageHandlerAdapter;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.string.StringDecoder;
import reactor.core.ComponentSpec;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.tcp.TcpConnection;
import reactor.tcp.TcpServer;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;

/**
 * @author Jon Brisbin
 */
public class SyslogServer extends TcpServer<SyslogMessage, SyslogMessage> {

	private SyslogServer(Environment env,
											 Reactor reactor,
											 InetSocketAddress listenAddress,
											 int backlog,
											 int rcvbuf,
											 int sndbuf,
											 Collection<Consumer<TcpConnection<SyslogMessage, SyslogMessage>>> connectionConsumers) {
		super(env, reactor, listenAddress, backlog, rcvbuf, sndbuf, null, connectionConsumers);
	}

	@Override
	protected ChannelHandler[] createChannelHandlers(SocketChannel ch) {
		final NettyTcpConnection<SyslogMessage, SyslogMessage> conn = registerConnection(ch);

		ChannelHandler frameDecoder = new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter());
		ChannelHandler stringDecoder = new StringDecoder();
		ChannelHandler syslogDecoder = new SyslogDecoder();
		ChannelHandler msgHandler = new ChannelInboundMessageHandlerAdapter<SyslogMessage>() {
			@Override
			public void messageReceived(ChannelHandlerContext ctx, SyslogMessage msg) throws Exception {
				SyslogServer.this.reactor.notify(conn.getReadKey(), Event.wrap(msg));
			}
		};

		return new ChannelHandler[]{
				frameDecoder,
				stringDecoder,
				syslogDecoder,
				msgHandler
		};
	}

	@Override
	public SyslogServer start() {
		super.start();
		return this;
	}

	@Override
	public SyslogServer start(Consumer<Void> started) {
		super.start(started);
		return this;
	}

	@Override
	public SyslogServer shutdown() {
		super.shutdown();
		return this;
	}

	private static final class SyslogDecoder extends MessageToMessageDecoder<String, SyslogMessage> {
		@Override
		public SyslogMessage decode(ChannelHandlerContext ctx, String msg) throws Exception {
			return SyslogMessageParser.parse(msg);
		}
	}

	public static class Spec extends ComponentSpec<Spec, SyslogServer> {
		private InetSocketAddress listenAddress = new InetSocketAddress(3000);
		private int               backlog       = 512;
		private int               rcvbuf        = 8 * 1024;
		private int               sndbuf        = 8 * 1024;
		private Collection<Consumer<TcpConnection<SyslogMessage, SyslogMessage>>> connectionConsumers;

		public Spec backlog(int backlog) {
			this.backlog = backlog;
			return this;
		}

		public Spec bufferSize(int rcvbuf, int sndbuf) {
			this.rcvbuf = rcvbuf;
			this.sndbuf = sndbuf;
			return this;
		}

		public Spec listen(int port) {
			this.listenAddress = new InetSocketAddress(port);
			return this;
		}

		public Spec listen(String host, int port) {
			this.listenAddress = new InetSocketAddress(host, port);
			return this;
		}

		public Spec consume(Consumer<TcpConnection<SyslogMessage, SyslogMessage>> connectionConsumer) {
			return consume(Collections.singletonList(connectionConsumer));
		}

		public Spec consume(Collection<Consumer<TcpConnection<SyslogMessage, SyslogMessage>>> connectionConsumers) {
			this.connectionConsumers = connectionConsumers;
			return this;
		}

		@Override
		protected SyslogServer configure(Reactor reactor) {
			return new SyslogServer(
					env,
					reactor,
					listenAddress,
					backlog,
					rcvbuf,
					sndbuf,
					connectionConsumers
			);
		}
	}

}
