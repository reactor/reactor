package reactor.tcp.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Environment;
import reactor.core.Promise;
import reactor.core.Promises;
import reactor.core.Reactor;
import reactor.io.Buffer;
import reactor.support.NamedDaemonThreadFactory;
import reactor.tcp.TcpClient;
import reactor.tcp.TcpConnection;
import reactor.tcp.encoding.Codec;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Jon Brisbin
 */
public class NettyTcpClient<IN, OUT> extends TcpClient<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(NettyTcpClient.class);
	private final Bootstrap bootstrap;
	private final Reactor   eventsReactor;
	private final int       memReclaimRatio;

	public NettyTcpClient(@Nonnull Environment env,
												@Nonnull Reactor reactor,
												@Nonnull InetSocketAddress connectAddress,
												int rcvbuf,
												int sndbuf,
												@Nullable Codec<Buffer, IN, OUT> codec) {
		super(env, reactor, connectAddress, rcvbuf, sndbuf, codec);
		this.eventsReactor = reactor;

		memReclaimRatio = env.getProperty("reactor.tcp.memReclaimRatio", Integer.class, 12);
		int ioThreadCount = env.getProperty("reactor.tcp.ioThreadCount", Integer.class, Environment.PROCESSORS);
		EventLoopGroup ioGroup = new NioEventLoopGroup(ioThreadCount, new NamedDaemonThreadFactory("reactor-tcp-io"));

		this.bootstrap = new Bootstrap()
				.group(ioGroup)
				.channel(NioSocketChannel.class)
				.option(ChannelOption.SO_RCVBUF, rcvbuf)
				.option(ChannelOption.SO_SNDBUF, sndbuf)
				.remoteAddress(connectAddress)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(final SocketChannel ch) throws Exception {
						ch.pipeline()
							.addLast(createChannelHandlers(ch));

						ch.closeFuture().addListener(new ChannelFutureListener() {
							@Override
							public void operationComplete(ChannelFuture future) throws Exception {
								close(ch);
							}
						});
					}
				});
	}

	@Override
	public Promise<TcpConnection<IN, OUT>> open() {
		final Promise<TcpConnection<IN, OUT>> p = Promises.<TcpConnection<IN, OUT>>defer()
																											.using(env)
																											.using(eventsReactor)
																											.get();

		ChannelFuture connectFuture = bootstrap.connect();
		connectFuture.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (future.isSuccess()) {
					final NettyTcpConnection<IN, OUT> conn = (NettyTcpConnection<IN, OUT>) select(future.channel());
					if (log.isInfoEnabled()) {
						log.info("CONNECT: " + conn);
					}
					p.set(conn);
				} else {
					p.set(future.cause());
				}
			}
		});

		return p;
	}

	@Override
	protected <C> TcpConnection<IN, OUT> createConnection(C channel) {
		SocketChannel ch = (SocketChannel) channel;
		int backlog = env.getProperty("reactor.tcp.connectionReactorBacklog", Integer.class, 128);

		return new NettyTcpConnection<IN, OUT>(
				env,
				getCodec(),
				new NettyEventLoopDispatcher(ch.eventLoop(), backlog),
				eventsReactor,
				ch
		);
	}

	protected ChannelHandler[] createChannelHandlers(SocketChannel ch) {
		final NettyTcpConnection<IN, OUT> conn = (NettyTcpConnection<IN, OUT>) select(ch);

		ChannelHandler readHandler = new ChannelInboundByteHandlerAdapter() {
			AtomicInteger counter = new AtomicInteger();

			@Override
			public void inboundBufferUpdated(ChannelHandlerContext ctx, ByteBuf data) throws Exception {
				Buffer b = new Buffer(data.nioBuffer());
				conn.read(b);

				if (counter.incrementAndGet() % memReclaimRatio == 0) {
					data.clear();
				}
			}

			@Override
			public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
				NettyTcpClient.this.notifyError(cause);
			}
		};

		return new ChannelHandler[]{readHandler};
	}

}
