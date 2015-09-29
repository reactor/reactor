/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.io.net.impl.netty.tcp;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.SocketChannelConfig;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.fn.timer.Timer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.ChannelStream;
import reactor.io.net.ReactorChannelHandler;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.impl.netty.NettyChannelHandlerBridge;
import reactor.io.net.impl.netty.NettyChannelStream;
import reactor.io.net.impl.netty.NettyNativeDetector;
import reactor.io.net.impl.netty.NettyServerSocketOptions;
import reactor.io.net.tcp.TcpServer;
import reactor.io.net.tcp.ssl.SSLEngineSupplier;
import reactor.rx.Promise;
import reactor.rx.Promises;

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
public class NettyTcpServer<IN, OUT> extends TcpServer<IN, OUT> {

	private final static Logger log = LoggerFactory.getLogger(NettyTcpServer.class);

	private final NettyServerSocketOptions nettyOptions;
	private final ServerBootstrap          bootstrap;
	private final EventLoopGroup           selectorGroup;
	private final EventLoopGroup           ioGroup;

	protected NettyTcpServer(Timer timer,
	                         InetSocketAddress listenAddress,
	                         final ServerSocketOptions options,
	                         final SslOptions sslOptions,
	                         Codec<Buffer, IN, OUT> codec) {
		super(timer, listenAddress, options, sslOptions, codec);

		if (options instanceof NettyServerSocketOptions) {
			this.nettyOptions = (NettyServerSocketOptions) options;
		} else {
			this.nettyOptions = null;
		}

		int selectThreadCount = DEFAULT_TCP_SELECT_COUNT;
		int ioThreadCount = DEFAULT_TCP_THREAD_COUNT;

		this.selectorGroup = new NioEventLoopGroup(selectThreadCount, new NamedDaemonThreadFactory
		  ("reactor-tcp-select"));

		if (null != nettyOptions && null != nettyOptions.eventLoopGroup()) {
			this.ioGroup = nettyOptions.eventLoopGroup();
		} else {
			this.ioGroup = NettyNativeDetector.newEventLoopGroup(ioThreadCount, new NamedDaemonThreadFactory("reactor-tcp-io"));
		}

		ServerBootstrap _serverBootstrap = new ServerBootstrap()
		  .group(selectorGroup, ioGroup)
		  .channel(NettyNativeDetector.getServerChannel())
		  .localAddress((null == listenAddress ? new InetSocketAddress(0) : listenAddress))
		  .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
		  .childOption(ChannelOption.AUTO_READ, sslOptions != null);

		if (options != null) {
			_serverBootstrap = _serverBootstrap
			  .option(ChannelOption.SO_BACKLOG, options.backlog())
			  .option(ChannelOption.SO_RCVBUF, options.rcvbuf())
			  .option(ChannelOption.SO_SNDBUF, options.sndbuf())
			  .option(ChannelOption.SO_REUSEADDR, options.reuseAddr());
		}

		this.bootstrap = _serverBootstrap;

	}

	@Override
	protected Promise<Void> doStart(final ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler) {

		bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(final SocketChannel ch) throws Exception {
				if (nettyOptions != null) {
					SocketChannelConfig config = ch.config();
					config.setReceiveBufferSize(nettyOptions.rcvbuf());
					config.setSendBufferSize(nettyOptions.sndbuf());
					config.setKeepAlive(nettyOptions.keepAlive());
					config.setReuseAddress(nettyOptions.reuseAddr());
					config.setSoLinger(nettyOptions.linger());
					config.setTcpNoDelay(nettyOptions.tcpNoDelay());
				}

				if (log.isDebugEnabled()) {
					log.debug("CONNECT {}", ch);
				}

				if (null != getSslOptions()) {
					SSLEngine ssl = new SSLEngineSupplier(getSslOptions(), false).get();
					if (log.isDebugEnabled()) {
						log.debug("SSL enabled using keystore {}",
						  (null != getSslOptions().keystoreFile() ? getSslOptions().keystoreFile() : "<DEFAULT>"));
					}
					ch.pipeline().addLast(new SslHandler(ssl));
				}

				if (null != nettyOptions && null != nettyOptions.pipelineConfigurer()) {
					nettyOptions.pipelineConfigurer().accept(ch.pipeline());
				}

				bindChannel(handler, ch);
			}
		});

		ChannelFuture bindFuture = bootstrap.bind();


		final Promise<Void> promise = Promises.ready();
		bindFuture.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				log.info("BIND {}", future.channel().localAddress());
				if (future.isSuccess()) {
					if (listenAddress.getPort() == 0) {
						listenAddress = (InetSocketAddress) future.channel().localAddress();
					}
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
	public Promise<Void> doShutdown() {

		final Promise<Void> d = Promises.ready();

		final AtomicInteger groupsToShutdown = new AtomicInteger(2);
		GenericFutureListener listener = new GenericFutureListener() {

			@Override
			public void operationComplete(Future future) throws Exception {
				if (groupsToShutdown.decrementAndGet() == 0) {
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

	protected void bindChannel(ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler, SocketChannel
	  nativeChannel) {

		NettyChannelStream<IN, OUT> netChannel = new NettyChannelStream<IN, OUT>(
		  getDefaultTimer(),
		  getDefaultCodec(),
		  getDefaultPrefetchSize(),
		  nativeChannel
		);

		ChannelPipeline pipeline = nativeChannel.pipeline();

		if (log.isDebugEnabled()) {
			pipeline.addLast(new LoggingHandler(NettyTcpServer.class));
		}
		pipeline.addLast(
		  new NettyChannelHandlerBridge<IN, OUT>(handler, netChannel)
		);
	}

}
