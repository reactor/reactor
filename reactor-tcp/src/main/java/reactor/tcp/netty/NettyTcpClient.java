/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import reactor.tcp.config.ClientSocketOptions;
import reactor.tcp.encoding.Codec;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;

/**
 * @author Jon Brisbin
 */
public class NettyTcpClient<IN, OUT> extends TcpClient<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(NettyTcpClient.class);
	private final Bootstrap           bootstrap;
	private final Reactor             eventsReactor;
	private final ClientSocketOptions options;

	public NettyTcpClient(@Nonnull Environment env,
												@Nonnull Reactor reactor,
												@Nonnull InetSocketAddress connectAddress,
												ClientSocketOptions opts,
												@Nullable Codec<Buffer, IN, OUT> codec) {
		super(env, reactor, connectAddress, opts, codec);
		this.eventsReactor = reactor;
		this.options = opts;

		int ioThreadCount = env.getProperty("reactor.tcp.ioThreadCount", Integer.class, Environment.PROCESSORS);
		EventLoopGroup ioGroup = new NioEventLoopGroup(ioThreadCount, new NamedDaemonThreadFactory("reactor-tcp-io"));

		this.bootstrap = new Bootstrap()
				.group(ioGroup)
				.channel(NioSocketChannel.class)
				.option(ChannelOption.SO_RCVBUF, options.rcvbuf())
				.option(ChannelOption.SO_SNDBUF, options.sndbuf())
				.option(ChannelOption.SO_KEEPALIVE, options.keepAlive())
				.option(ChannelOption.SO_LINGER, options.linger())
				.option(ChannelOption.TCP_NODELAY, options.tcpNoDelay())
				.remoteAddress(connectAddress)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(final SocketChannel ch) throws Exception {
						ch.config().setConnectTimeoutMillis(options.timeout());
						ch.pipeline().addLast(createChannelHandlers(ch));

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
			@Override
			public void inboundBufferUpdated(ChannelHandlerContext ctx, ByteBuf data) throws Exception {
				Buffer b = new Buffer(data.nioBuffer());
				int start = b.position();
				conn.read(b);
				data.skipBytes(b.position() - start);
			}

			@Override
			public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
				NettyTcpClient.this.notifyError(cause);
			}
		};

		return new ChannelHandler[]{readHandler};
	}

}
