package reactor.tcp.netty;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;

import java.net.SocketAddress;

/**
 * A simple {@link ChannelHandlerAdapter} that logs when a server is bound, a client is connected, or an exception is
 * thrown.
 *
 * @author Jon Brisbin
 */
public class LoggingHandler extends ChannelHandlerAdapter {

	private final Logger log;

	public LoggingHandler(Logger log) {
		this.log = log;
	}

	@Override
	public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelFuture future) throws Exception {
		if (log.isInfoEnabled()) {
			log.info("BIND {}", localAddress);
		}
		super.bind(ctx, localAddress, future);
	}

	@Override
	public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelFuture future) throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("CONNECT {}", remoteAddress);
		}
		super.connect(ctx, remoteAddress, localAddress, future);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (log.isErrorEnabled()) {
			log.error(cause.getMessage(), cause);
		}
	}

}
