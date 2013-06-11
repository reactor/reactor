package reactor.tcp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.socket.SocketChannel;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.dispatch.Dispatcher;
import reactor.io.Buffer;
import reactor.tcp.AbstractTcpConnection;
import reactor.tcp.encoding.Codec;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * {@link reactor.tcp.TcpConnection} implementation that uses Netty.
 *
 * @author Jon Brisbin
 */
class NettyTcpConnection<IN, OUT> extends AbstractTcpConnection<IN, OUT> {

	private final SocketChannel     channel;
	private final InetSocketAddress remoteAddress;

	NettyTcpConnection(Environment env,
										 Codec<Buffer, IN, OUT> codec,
										 Dispatcher ioDispatcher,
										 Reactor eventsReactor,
										 SocketChannel channel) {
		super(env, codec, ioDispatcher, eventsReactor);
		this.channel = channel;
		this.remoteAddress = channel.remoteAddress();
	}

	@Override
	public boolean consumable() {
		return !channel.isInputShutdown();
	}

	@Override
	public boolean writable() {
		return !channel.isOutputShutdown();
	}

	@Override
	public InetSocketAddress remoteAddress() {
		return remoteAddress;
	}

	@Override
	protected void write(Buffer data, final Consumer<Boolean> onComplete) {
		write(data.byteBuffer(), onComplete);
	}

	protected void write(ByteBuffer data, final Consumer<Boolean> onComplete) {
		ByteBuf buf = channel.alloc().buffer(data.remaining());
		buf.writeBytes(data);

		write(buf, onComplete);
	}

	@Override
	protected void write(Object data, final Consumer<Boolean> onComplete) {
		channel.write(data);
		ChannelFuture writeFuture = channel.flush();
		if (null != onComplete) {
			writeFuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					onComplete.accept(future.isSuccess());
				}
			});
		}
	}

}
