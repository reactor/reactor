package reactor.net.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.io.Buffer;
import reactor.net.AbstractNetChannel;

/**
 * Netty {@link io.netty.channel.ChannelInboundHandler} implementation that passes data to a Reactor {@link
 * reactor.net.AbstractNetChannel}.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public class NettyNetChannelInboundHandler extends ChannelInboundHandlerAdapter {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private volatile AbstractNetChannel netChannel;
	private volatile ByteBuf            remainder;

	public NettyNetChannelInboundHandler() {
	}

	public AbstractNetChannel getNetChannel() {
		return netChannel;
	}

	public NettyNetChannelInboundHandler setNetChannel(AbstractNetChannel netChannel) {
		this.netChannel = netChannel;
		return this;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (!ByteBuf.class.isInstance(msg) || null == netChannel.getDecoder()) {
			netChannel.notifyRead(msg);
			return;
		}

		ByteBuf data = (ByteBuf) msg;
		if (remainder == null) {
			try {
				passToConnection(data);
			} finally {
				if (data.isReadable()) {
					remainder = data;
				} else {
					data.release();
				}
			}
			return;
		}

		if (!bufferHasSufficientCapacity(remainder, data)) {
			ByteBuf combined = createCombinedBuffer(remainder, data, ctx);
			remainder.release();
			remainder = combined;
		} else {
			remainder.writeBytes(data);
		}
		data.release();

		try {
			passToConnection(remainder);
		} finally {
			if (remainder.isReadable()) {
				remainder.discardSomeReadBytes();
			} else {
				remainder.release();
				remainder = null;
			}
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if ("Broken pipe".equals(cause.getMessage()) || "Connection reset by peer".equals(cause.getMessage())) {
			if (log.isDebugEnabled()) {
				log.debug(ctx.channel().toString() + " " + cause.getMessage());
			}
		}
		netChannel.notifyError(cause);
		ctx.close();
	}

	private boolean bufferHasSufficientCapacity(ByteBuf receiver, ByteBuf provider) {
		return receiver.writerIndex() <= receiver.maxCapacity() - provider.readableBytes();
	}

	private ByteBuf createCombinedBuffer(ByteBuf partOne, ByteBuf partTwo, ChannelHandlerContext ctx) {
		ByteBuf combined = ctx.alloc().buffer(partOne.readableBytes() + partTwo.readableBytes());
		combined.writeBytes(partOne);
		combined.writeBytes(partTwo);
		return combined;
	}

	private void passToConnection(ByteBuf data) {
		Buffer b = new Buffer(data.nioBuffer());
		int start = b.position();
		netChannel.read(b);
		data.skipBytes(b.position() - start);
	}

}
