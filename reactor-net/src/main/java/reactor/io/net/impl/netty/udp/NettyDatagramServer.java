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

package reactor.io.net.impl.netty.udp;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ChannelFactory;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.InternetProtocolFamily;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.NetUtil;
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
import reactor.io.net.impl.netty.NettyChannelHandlerBridge;
import reactor.io.net.impl.netty.NettyChannelStream;
import reactor.io.net.impl.netty.NettyNativeDetector;
import reactor.io.net.impl.netty.NettyServerSocketOptions;
import reactor.io.net.udp.DatagramServer;
import reactor.rx.Promise;
import reactor.rx.Promises;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ProtocolFamily;

/**
 * {@link reactor.io.net.udp.DatagramServer} implementation built on Netty.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyDatagramServer<IN, OUT> extends DatagramServer<IN, OUT> {

	private final static Logger log = LoggerFactory.getLogger(NettyDatagramServer.class);

	private final    NettyServerSocketOptions nettyOptions;
	private final    Bootstrap                bootstrap;
	private final    EventLoopGroup           ioGroup;
	private volatile NioDatagramChannel       channel;

	public NettyDatagramServer(Timer timer,
	                           InetSocketAddress listenAddress,
	                           final NetworkInterface multicastInterface,
	                           final ServerSocketOptions options,
	                           Codec<Buffer, IN, OUT> codec) {
		super(timer, listenAddress, multicastInterface, options, codec);

		if (options instanceof NettyServerSocketOptions) {
			this.nettyOptions = (NettyServerSocketOptions) options;
		} else {
			this.nettyOptions = null;
		}

		if (null != nettyOptions && null != nettyOptions.eventLoopGroup()) {
			this.ioGroup = nettyOptions.eventLoopGroup();
		} else {
			int ioThreadCount = DEFAULT_UDP_THREAD_COUNT;
			this.ioGroup =
			  options == null || options.protocolFamily() == null ?
				NettyNativeDetector.newEventLoopGroup(ioThreadCount, new NamedDaemonThreadFactory
				  ("reactor-udp-io")) :
				new NioEventLoopGroup(ioThreadCount, new NamedDaemonThreadFactory
				  ("reactor-udp-io"));
		}


		this.bootstrap = new Bootstrap()
		  .group(ioGroup)
		  .option(ChannelOption.AUTO_READ, false)
		;

		if ((options == null ||
		  options.protocolFamily() == null)
		  &&
		  NettyNativeDetector.getDatagramChannel(ioGroup).equals(EpollDatagramChannel.class)) {
			bootstrap.channel(EpollDatagramChannel.class);
		} else {
			bootstrap.channelFactory(new ChannelFactory<Channel>() {
				@Override
				public Channel newChannel() {
					return new NioDatagramChannel(toNettyFamily(options != null ? options.protocolFamily() : null));
				}
			});
		}

		if (options != null) {
			bootstrap.option(ChannelOption.SO_RCVBUF, options.rcvbuf())
			  .option(ChannelOption.SO_SNDBUF, options.sndbuf())
			  .option(ChannelOption.SO_REUSEADDR, options.reuseAddr())
			  .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, options.timeout());
		}

		if (null != listenAddress) {
			bootstrap.localAddress(listenAddress);
		} else {
			bootstrap.localAddress(NetUtil.LOCALHOST, 3000);
		}
		if (null != multicastInterface) {
			bootstrap.option(ChannelOption.IP_MULTICAST_IF, multicastInterface);
		}
	}

	private InternetProtocolFamily toNettyFamily(ProtocolFamily family) {
		if (family == null) {
			return null;
		}
		switch (family.name()) {
			case "INET":
				return InternetProtocolFamily.IPv4;
			case "INET6":
				return InternetProtocolFamily.IPv6;
			default:
				throw new IllegalArgumentException("Unsupported protocolFamily: " + family.name());
		}
	}


	@SuppressWarnings("unchecked")
	@Override
	protected Promise<Void> doStart(final ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> channelHandler) {
		final Promise<Void> promise = Promises.ready(getDefaultTimer());

		ChannelFuture future = bootstrap.handler(new ChannelInitializer<DatagramChannel>() {
			@Override
			public void initChannel(final DatagramChannel ch) throws Exception {
				if (null != nettyOptions && null != nettyOptions.pipelineConfigurer()) {
					nettyOptions.pipelineConfigurer().accept(ch.pipeline());
				}

				bindChannel(channelHandler, ch);
			}
		}).bind();
		future.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (future.isSuccess()) {
					log.info("BIND {}", future.channel().localAddress());
					channel = (NioDatagramChannel) future.channel();
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
	protected Promise<Void> doShutdown() {
		final Promise<Void> d = Promises.ready();

		ChannelFuture future = channel.close();
		final GenericFutureListener listener = new GenericFutureListener() {
			@Override
			public void operationComplete(Future future) throws Exception {
				if (future.isSuccess()) {
					d.onComplete();
				} else {
					d.onError(future.cause());
				}
			}
		};

		future.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (!future.isSuccess()) {
					d.onError(future.cause());
					return;
				}

				if (null == nettyOptions || null == nettyOptions.eventLoopGroup()) {
					ioGroup.shutdownGracefully().addListener(listener);
				}
			}
		});

		return d;
	}

	@Override
	public Promise<Void> join(final InetAddress multicastAddress, NetworkInterface iface) {
		if (null == channel) {
			throw new IllegalStateException("DatagramServer not running.");
		}

		final Promise<Void> d = Promises.ready(getDefaultTimer());

		if (null == iface && null != getMulticastInterface()) {
			iface = getMulticastInterface();
		}

		final ChannelFuture future;
		if (null != iface) {
			future = channel.joinGroup(new InetSocketAddress(multicastAddress, getListenAddress().getPort()), iface);
		} else {
			future = channel.joinGroup(multicastAddress);
		}
		future.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				log.info("JOIN {}", multicastAddress);
				if (future.isSuccess()) {
					d.onComplete();
				} else {
					d.onError(future.cause());
				}
			}
		});

		return d;
	}

	@Override
	public Promise<Void> leave(final InetAddress multicastAddress, NetworkInterface iface) {
		if (null == channel) {
			throw new IllegalStateException("DatagramServer not running.");
		}

		if (null == iface && null != getMulticastInterface()) {
			iface = getMulticastInterface();
		}

		final Promise<Void> d = Promises.ready(getDefaultTimer());

		final ChannelFuture future;
		if (null != iface) {
			future = channel.leaveGroup(new InetSocketAddress(multicastAddress, getListenAddress().getPort()), iface);
		} else {
			future = channel.leaveGroup(multicastAddress);
		}
		future.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				log.info("LEAVE {}", multicastAddress);
				if (future.isSuccess()) {
					d.onComplete();
				} else {
					d.onError(future.cause());
				}
			}
		});

		return d;
	}

	protected void bindChannel(ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler,
	                           Object _ioChannel) {
		NioDatagramChannel ioChannel = (NioDatagramChannel) _ioChannel;
		NettyChannelStream<IN, OUT> netChannel = new NettyChannelStream<IN, OUT>(
		  getDefaultTimer(),
		  getDefaultCodec(),
		  getDefaultPrefetchSize(),
		  ioChannel
		);

		ChannelPipeline pipeline = ioChannel.pipeline();

		if (log.isDebugEnabled()) {
			pipeline.addLast(new LoggingHandler(NettyDatagramServer.class));
		}

		pipeline.addLast(
		  new NettyChannelHandlerBridge<IN, OUT>(handler, netChannel) {
			  @Override
			  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
				  if (msg != null && DatagramPacket.class.isAssignableFrom(msg.getClass())) {
					  super.channelRead(ctx, ((DatagramPacket) msg).content());
				  } else {
					  super.channelRead(ctx, msg);
				  }
			  }
		  },
		  new ChannelOutboundHandlerAdapter() {
			  @Override
			  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
				  super.write(ctx, msg, promise);
			  }
		  });
	}
}
