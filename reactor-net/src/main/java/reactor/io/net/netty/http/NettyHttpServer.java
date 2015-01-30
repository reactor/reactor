/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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

package reactor.io.net.netty.http;

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
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.http.HttpServer;
import reactor.io.net.netty.*;
import reactor.io.net.tcp.ssl.SSLEngineSupplier;
import reactor.rx.Promise;
import reactor.rx.Promises;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Netty-based {@code TcpServer} implementation
 *
 * @param <IN>  The type that will be received by this server
 * @param <OUT> The type that will be sent by this server
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyHttpServer<IN, OUT> extends HttpServer<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final NettyServerSocketOptions nettyOptions;
	private final ServerBootstrap          bootstrap;
	private final EventLoopGroup           selectorGroup;
	private final EventLoopGroup           ioGroup;

	protected NettyHttpServer(@Nonnull Environment env,
	                          @Nonnull Dispatcher dispatcher,
	                          @Nullable InetSocketAddress listenAddress,
	                          final ServerSocketOptions options,
	                          final SslOptions sslOptions,
	                          @Nullable Codec<Buffer, IN, OUT> codec) {
		super(env, dispatcher, listenAddress, options, sslOptions, codec);

		if (options instanceof NettyServerSocketOptions) {
			this.nettyOptions = (NettyServerSocketOptions) options;
		} else {
			this.nettyOptions = null;
		}

		int selectThreadCount = env.getProperty("reactor.tcp.selectThreadCount", Integer.class,
				Environment.PROCESSORS / 2);
		int ioThreadCount = env.getProperty("reactor.tcp.ioThreadCount", Integer.class, Environment.PROCESSORS);
		this.selectorGroup = new NioEventLoopGroup(selectThreadCount, new NamedDaemonThreadFactory("reactor-tcp-select"));
		if (null != nettyOptions && null != nettyOptions.eventLoopGroup()) {
			this.ioGroup = nettyOptions.eventLoopGroup();
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
						if(options.prefetch() != -1 && options.prefetch() != Long.MAX_VALUE){
							ch.config().setAutoRead(false);
						}

						if (null != sslOptions) {
							SSLEngine ssl = new SSLEngineSupplier(sslOptions, false).get();
							if (log.isDebugEnabled()) {
								log.debug("SSL enabled using keystore {}",
										(null != sslOptions.keystoreFile() ? sslOptions.keystoreFile() : "<DEFAULT>"));
							}
							ch.pipeline().addLast(new SslHandler(ssl));
						}
						if (null != nettyOptions && null != nettyOptions.pipelineConfigurer()) {
							nettyOptions.pipelineConfigurer().accept(ch.pipeline());
						}

						final NettyChannelStream<IN, OUT> netChannel = createChannel(ch, options.prefetch());
						notifyNewChannel(netChannel);

						mergeWrite(netChannel);

						ch.pipeline().addLast(
								new NettyNetChannelInboundHandler<IN>(
										netChannel.in(), netChannel),
								new NettyNetChannelOutboundHandler()
						);

					}
				});
	}

	@Override
	public Promise<Void> start() {
		ChannelFuture bindFuture = bootstrap.bind();
		final Promise<Void> promise = Promises.ready(getEnvironment(), getDispatcher());
		bindFuture.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				log.info("BIND {}", future.channel().localAddress());
				if (future.isSuccess()) {
					notifyStart();
					promise.onComplete();
				} else {
					promise.onError(future.cause());
				}
			}
		});

		return promise;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Promise<Void> shutdown() {
		final Promise<Void> d = Promises.ready(getEnvironment(), getDispatcher());

		final AtomicInteger groupsToShutdown = new AtomicInteger(2);
		GenericFutureListener listener = new GenericFutureListener() {

			@Override
			public void operationComplete(Future future) throws Exception {
				if (groupsToShutdown.decrementAndGet() == 0) {
					notifyShutdown();
					d.onComplete();
				}
			}
		};
		selectorGroup.shutdownGracefully().addListener(listener);
		if (null == nettyOptions || null == nettyOptions.eventLoopGroup()) {
			ioGroup.shutdownGracefully().addListener(listener);
		}

		return d;
	}

	@Override
	protected NettyChannelStream<IN, OUT> createChannel(Object nativeChannel, long prefetch) {
		return new NettyChannelStream<IN, OUT>(
				getEnvironment(),
				getDefaultCodec(),
				prefetch == -1l ? getPrefetchSize() : prefetch,
				this,
				new NettyEventLoopDispatcher(((Channel) nativeChannel).eventLoop(), 256),
				getDispatcher(),
				(Channel) nativeChannel
		);
	}

}
