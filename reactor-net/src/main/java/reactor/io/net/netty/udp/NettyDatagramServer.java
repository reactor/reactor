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

package reactor.io.net.netty.udp;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ChannelFactory;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannelConfig;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.NetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Environment;
import reactor.bus.EventBus;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.fn.Consumer;
import reactor.fn.batch.BatchConsumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.NetChannel;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.netty.NettyNetChannel;
import reactor.io.net.netty.NettyNetChannelInboundHandler;
import reactor.io.net.netty.NettyServerSocketOptions;
import reactor.io.net.udp.DatagramServer;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.Collection;

/**
 * {@link reactor.io.net.udp.DatagramServer} implementation built on Netty.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyDatagramServer<IN, OUT> extends DatagramServer<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final    NettyServerSocketOptions nettyOptions;
	private final    Bootstrap                bootstrap;
	private final    EventLoopGroup           ioGroup;
	private volatile NioDatagramChannel       channel;
	private volatile NettyNetChannel<IN, OUT> netChannel;

	public NettyDatagramServer(@Nonnull Environment env,
	                           @Nonnull EventBus reactor,
	                           @Nullable InetSocketAddress listenAddress,
	                           @Nullable final NetworkInterface multicastInterface,
	                           @Nonnull final ServerSocketOptions options,
	                           @Nullable Codec<Buffer, IN, OUT> codec,
	                           Collection<Consumer<NetChannel<IN, OUT>>> consumers) {
		super(env, reactor, listenAddress, multicastInterface, options, codec, consumers);

		if (options instanceof NettyServerSocketOptions) {
			this.nettyOptions = (NettyServerSocketOptions) options;
		} else {
			this.nettyOptions = null;
		}

		if (null != nettyOptions && null != nettyOptions.eventLoopGroup()) {
			this.ioGroup = nettyOptions.eventLoopGroup();
		} else {
			int ioThreadCount = env.getProperty("reactor.udp.ioThreadCount",
					Integer.class,
					Environment.PROCESSORS);
			this.ioGroup = new NioEventLoopGroup(ioThreadCount, new NamedDaemonThreadFactory("reactor-udp-io"));
		}

		final NettyNetChannelInboundHandler inboundHandler = new NettyNetChannelInboundHandler() {
			@Override
			public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
				super.channelRead(ctx, ((DatagramPacket) msg).content());
			}
		};

		this.bootstrap = new Bootstrap()
				.group(ioGroup)
				.option(ChannelOption.SO_RCVBUF, options.rcvbuf())
				.option(ChannelOption.SO_SNDBUF, options.sndbuf())
				.option(ChannelOption.SO_REUSEADDR, options.reuseAddr())
				.channelFactory(new ChannelFactory<Channel>() {
					@Override
					public Channel newChannel() {
						final NioDatagramChannel ch = new NioDatagramChannel();
						DatagramChannelConfig config = ch.config();
						config.setReceiveBufferSize(options.rcvbuf());
						config.setSendBufferSize(options.sndbuf());
						config.setReuseAddress(options.reuseAddr());

						if (null != multicastInterface) {
							config.setNetworkInterface(multicastInterface);
						}

						if (null != nettyOptions && null != nettyOptions.pipelineConfigurer()) {
							nettyOptions.pipelineConfigurer().accept(ch.pipeline());
						}

						ch.closeFuture().addListener(new ChannelFutureListener() {
							@Override
							public void operationComplete(ChannelFuture future) throws Exception {
								if (log.isInfoEnabled()) {
									log.info("CLOSE {}", ch);
								}
								close(ch);
							}
						});

						netChannel = (NettyNetChannel<IN, OUT>) select(ch);
						inboundHandler.setNetChannel(netChannel);

						ch.pipeline().addLast(new ChannelOutboundHandlerAdapter() {
							@Override
							public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
								super.write(ctx, msg, promise);
							}
						});

						return ch;
					}
				})
				.handler(inboundHandler);

		if (null != listenAddress) {
			bootstrap.localAddress(listenAddress);
		} else {
			bootstrap.localAddress(NetUtil.LOCALHOST, 3000);
		}
		if (null != multicastInterface) {
			bootstrap.option(ChannelOption.IP_MULTICAST_IF, multicastInterface);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public DatagramServer<IN, OUT> start(@Nullable final Runnable started) {
		ChannelFuture future = bootstrap.bind();
		if (null != started) {
			future.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if (future.isSuccess()) {
						log.info("BIND {}", future.channel().localAddress());
						notifyStart(started);
						channel = (NioDatagramChannel) future.channel();
					}
				}
			});
		}
		return this;
	}

	@Override
	public Promise<Boolean> shutdown() {
		final Promise<Boolean> d = Promises.ready(getEnvironment(), getReactor().getDispatcher());

		getReactor().schedule(
				new Consumer<Void>() {
					@SuppressWarnings("unchecked")
					@Override
					public void accept(Void v) {
						GenericFutureListener listener = new GenericFutureListener() {
							@Override
							public void operationComplete(Future future) throws Exception {
								if (future.isSuccess()) {
									d.onNext(true);
								} else {
									d.onError(future.cause());
								}
							}
						};
						if (null == nettyOptions || null == nettyOptions.eventLoopGroup()) {
							ioGroup.shutdownGracefully().addListener(listener);
						}
					}
				},
				null
		);
		notifyShutdown();

		return d;
	}

	@Override
	public DatagramServer<IN, OUT> send(OUT data) {
		if (null == channel) {
			throw new IllegalStateException("DatagramServer not running.");
		}

		netChannel.send(data);

		return this;
	}

	@Override
	public Stream<IN> in() {
		return netChannel.in();
	}

	@Override
	public BatchConsumer<OUT> out() {
		return netChannel.out();
	}

	@Override
	public Promise<Void> join(InetAddress multicastAddress, NetworkInterface iface) {
		if (null == channel) {
			throw new IllegalStateException("DatagramServer not running.");
		}

		final Promise<Void> d = Promises.ready(getEnvironment(), getReactor().getDispatcher());

		if (null == iface && null != getMulticastInterface()) {
			iface = getMulticastInterface();
		}

		final ChannelFuture future;
		if (null != iface) {
			future = channel.joinGroup(new InetSocketAddress(multicastAddress, getListenAddress().getPort()), iface);
		} else {
			future = channel.joinGroup(multicastAddress);
		}
		future.addListener(new PromiseCompletingListener(d));

		return d;
	}

	@Override
	public Promise<Void> leave(InetAddress multicastAddress, NetworkInterface iface) {
		if (null == channel) {
			throw new IllegalStateException("DatagramServer not running.");
		}

		if (null == iface && null != getMulticastInterface()) {
			iface = getMulticastInterface();
		}

		final Promise<Void> d = Promises.ready(getEnvironment(), getReactor().getDispatcher());

		final ChannelFuture future;
		if (null != iface) {
			future = channel.leaveGroup(new InetSocketAddress(multicastAddress, getListenAddress().getPort()), iface);
		} else {
			future = channel.leaveGroup(multicastAddress);
		}
		future.addListener(new PromiseCompletingListener(d));

		return d;
	}

	@Override
	protected <C> NetChannel<IN, OUT> createChannel(C ioChannel) {
		return new NettyNetChannel<IN, OUT>(
				getEnvironment(),
				getCodec(),
				SynchronousDispatcher.INSTANCE,
				getReactor(),
				(NioDatagramChannel) ioChannel
		);
	}

	@Override
	protected void doClose(@Nullable final Consumer<Boolean> onClose) {
		ChannelFuture future = channel.close();
		if (null != onClose) {
			future.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					getReactor().schedule(onClose, future.isDone() && future.isSuccess());
				}
			});
		}
	}

	private static class PromiseCompletingListener implements ChannelFutureListener {
		private final Promise<Void> d;

		private PromiseCompletingListener(Promise<Void> d) {
			this.d = d;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (future.isSuccess()) {
				d.onComplete();
			} else {
				d.onError(future.cause());
			}
		}
	}

}
