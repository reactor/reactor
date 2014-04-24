package reactor.net.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import reactor.tuple.Tuple2;

/**
 * @author Jon Brisbin
 */
public class NettyNetChannelOutboundHandler extends ChannelOutboundHandlerAdapter {
	@SuppressWarnings("unchecked")
	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (Tuple2.class.isInstance(msg)) {
			Tuple2<Object, Boolean> tup = (Tuple2<Object, Boolean>) msg;
			if (null != tup.getT1()) {
				super.write(ctx, tup.getT1(), promise);
			}
			if (tup.getT2()) {
				ctx.flush();
			}
		} else {
			super.write(ctx, msg, promise);
			ctx.flush();
		}
	}
}
