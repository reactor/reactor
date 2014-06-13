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

package reactor.net.netty.tcp;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.function.Consumer;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;
import reactor.net.AbstractNetChannel;
import reactor.net.NetChannel;
import reactor.net.config.ServerSocketOptions;
import reactor.net.config.SslOptions;
import reactor.net.netty.*;
import reactor.net.tcp.TcpServer;
import reactor.net.tcp.ssl.SSLEngineSupplier;
import reactor.support.NamedDaemonThreadFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Netty-based {@code TcpServer} implementation
 *
 * @param <IN>
 * 		The type that will be received by this server
 * @param <OUT>
 * 		The type that will be sent by this server
 *
 * @author Jon Brisbin
 */
public class NettyTcpServer<IN, OUT> extends TcpServer<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final ServerBootstrap bootstrap;
	private final EventLoopGroup  selectorGroup;
	private final EventLoopGroup  ioGroup;

	protected NettyTcpServer(@Nonnull Environment env,
	                         @Nonnull Reactor reactor,
	                         @Nullable InetSocketAddress listenAddress,
	                         final ServerSocketOptions options,
	                         final SslOptions sslOptions,
	                         @Nullable Codec<Buffer, IN, OUT> codec,
	                         @Nonnull Collection<Consumer<NetChannel<IN, OUT>>> consumers) {
		super(env, reactor, listenAddress, options, sslOptions, codec, consumers);

		int selectThreadCount = env.getProperty("reactor.tcp.selectThreadCount", Integer.class,
		                                        Environment.PROCESSORS / 2);
		int ioThreadCount = env.getProperty("reactor.tcp.ioThreadCount", Integer.class, Environment.PROCESSORS);
		this.selectorGroup = new NioEventLoopGroup(selectThreadCount, new NamedDaemonThreadFactory("reactor-tcp-select"));
		if (null != options
				&& options instanceof NettyServerSocketOptions
				&& null != ((NettyServerSocketOptions) options).eventLoopGroup()) {
			this.ioGroup = ((NettyServerSocketOptions) options).eventLoopGroup();
		} else {
			this.ioGroup = new NioEventLoopGroup(ioThreadCount, new NamedDaemonThreadFactory("reactor-tcp-io"));
		}

		this.bootstrap = new ServerBootstrap()
				.group(selectorGroup, ioGroup)
				.channel(NioServerSocketChannel.class)
				.option(ChannelOption.SO_BACKLOG, options.backlog())
				.option(ChannelOption.SO_RCVBUF, options.rcvbuf())
				.option(ChannelOption.SO_SNDBUF, options.sndbuf())
				.option(ChannelOption.SO_REUSEADDR, options.reuseAddr())
				.localAddress((null == listenAddress ? new InetSocketAddress(3000) : listenAddress))
				.childHandler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(final SocketChannel ch) throws Exception {
						SocketChannelConfig config = ch.config();
						config.setReceiveBufferSize(options.rcvbuf());
						config.setSendBufferSize(options.sndbuf());
						config.setKeepAlive(options.keepAlive());
						config.setReuseAddress(options.reuseAddr());
						config.setSoLinger(options.linger());
						config.setTcpNoDelay(options.tcpNoDelay());

						if (log.isDebugEnabled()) {
							log.debug("CONNECT {}", ch);
						}

						if (null != sslOptions) {
							SSLEngine ssl = new SSLEngineSupplier(sslOptions, false).get();
							if (log.isDebugEnabled()) {
								log.debug("SSL enabled using keystore {}",
								          (null != sslOptions.keystoreFile() ? sslOptions.keystoreFile() : "<DEFAULT>"));
							}
							ch.pipeline().addLast(new SslHandler(ssl));
						}
						if (options instanceof NettyServerSocketOptions
								&& null != ((NettyServerSocketOptions) options).pipelineConfigurer()) {
							((NettyServerSocketOptions) options).pipelineConfigurer().accept(ch.pipeline());
						}
						ch.pipeline().addLast(createChannelHandlers(ch));
						ch.closeFuture().addListener(new ChannelFutureListener() {
							@Override
							public void operationComplete(ChannelFuture future) throws Exception {
								if (log.isDebugEnabled()) {
									log.debug("CLOSE {}", ch);
								}
								close(ch);
							}
						});
					}
				});
	}

	@Override
	public TcpServer<IN, OUT> start(@Nullable final Runnable started) {
		ChannelFuture bindFuture = bootstrap.bind();
		if (null != started) {
			bindFuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					log.info("BIND {}", future.channel().localAddress());
					notifyStart(started);
				}
			});
		}

		return this;
	}

	@Override
	public Promise<Boolean> shutdown() {
		final Deferred<Boolean, Promise<Boolean>> d = Promises.defer(getEnvironment(), getReactor().getDispatcher());
		getReactor().schedule(
				new Consumer<Void>() {
					@SuppressWarnings({"rawtypes", "unchecked"})
					@Override
					public void accept(Void v) {
						final AtomicInteger groupsToShutdown = new AtomicInteger(2);
						GenericFutureListener listener = new GenericFutureListener() {

							@Override
							public void operationComplete(Future future) throws Exception {
								if (groupsToShutdown.decrementAndGet() == 0) {
									notifyShutdown();
									d.accept(true);
								}
							}
						};
						selectorGroup.shutdownGracefully().addListener(listener);
						ioGroup.shutdownGracefully().addListener(listener);
					}
				},
				null
		);

		return d.compose();
	}

	@Override
	protected <C> NetChannel<IN, OUT> createChannel(C ioChannel) {
		return new NettyNetChannel<IN, OUT>(
				getEnvironment(),
				getCodec(),
				new NettyEventLoopDispatcher(((Channel) ioChannel).eventLoop(), 256),
				getReactor(),
				(Channel) ioChannel
		);
	}

	protected ChannelHandler[] createChannelHandlers(SocketChannel ch) {
		AbstractNetChannel<IN, OUT> netChannel = (AbstractNetChannel<IN, OUT>) select(ch);
		NettyNetChannelInboundHandler readHandler = new NettyNetChannelInboundHandler()
				.setNetChannel(netChannel);
		NettyNetChannelOutboundHandler writeHandler = new NettyNetChannelOutboundHandler();

		return new ChannelHandler[]{readHandler, writeHandler};
	}

}
