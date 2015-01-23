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

package reactor.io.net.netty.tcp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.dispatch.SynchronousDispatcher;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.fn.tuple.Tuple2;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.NetChannelStream;
import reactor.io.net.Reconnect;
import reactor.io.net.config.ClientSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.netty.*;
import reactor.io.net.tcp.TcpClient;
import reactor.io.net.tcp.ssl.SSLEngineSupplier;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Stream;
import reactor.rx.broadcast.Broadcaster;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Netty-based {@code TcpClient}.
 *
 * @param <IN>  The type that will be received by this client
 * @param <OUT> The type that will be sent by this client
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyTcpClient<IN, OUT> extends TcpClient<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(NettyTcpClient.class);

	private final NettyClientSocketOptions nettyOptions;
	private final Bootstrap                bootstrap;
	private final EventLoopGroup           ioGroup;
	private final Supplier<ChannelFuture>  connectionSupplier;

	private volatile InetSocketAddress connectAddress;
	private volatile boolean           closing;

	/**
	 * Creates a new NettyTcpClient that will use the given {@code env} for configuration and the given {@code
	 * reactor} to
	 * send events. The number of IO threads used by the client is configured by the environment's {@code
	 * reactor.tcp.ioThreadCount} property. In its absence the number of IO threads will be equal to the {@link
	 * Environment#PROCESSORS number of available processors}. </p> The client will connect to the given {@code
	 * connectAddress}, configuring its socket using the given {@code opts}. The given {@code codec} will be used for
	 * encoding and decoding of data.
	 *
	 * @param env            The configuration environment
	 * @param dispatcher     The dispatcher used to send events
	 * @param connectAddress The address the client will connect to
	 * @param options        The configuration options for the client's socket
	 * @param sslOptions     The SSL configuration options for the client's socket
	 * @param codec          The codec used to encode and decode data
	 */
	public NettyTcpClient(@Nonnull Environment env,
	                      @Nonnull Dispatcher dispatcher,
	                      @Nonnull InetSocketAddress connectAddress,
	                      @Nonnull final ClientSocketOptions options,
	                      @Nullable final SslOptions sslOptions,
	                      @Nullable Codec<Buffer, IN, OUT> codec) {
		super(env, dispatcher, connectAddress, options, sslOptions, codec);
		this.connectAddress = connectAddress;

		if (options instanceof NettyClientSocketOptions) {
			this.nettyOptions = (NettyClientSocketOptions) options;
		} else {
			this.nettyOptions = null;

		}
		if (null != nettyOptions && null != nettyOptions.eventLoopGroup()) {
			this.ioGroup = nettyOptions.eventLoopGroup();
		} else {
			int ioThreadCount = env.getProperty("reactor.tcp.ioThreadCount", Integer.class, Environment.PROCESSORS);
			this.ioGroup = new NioEventLoopGroup(ioThreadCount, new NamedDaemonThreadFactory("reactor-tcp-io"));
		}

		this.bootstrap = new Bootstrap()
				.group(ioGroup)
				.channel(NioSocketChannel.class)
				.option(ChannelOption.SO_RCVBUF, options.rcvbuf())
				.option(ChannelOption.SO_SNDBUF, options.sndbuf())
				.option(ChannelOption.SO_KEEPALIVE, options.keepAlive())
				.option(ChannelOption.SO_LINGER, options.linger())
				.option(ChannelOption.TCP_NODELAY, options.tcpNoDelay())
				.remoteAddress(this.connectAddress)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(final SocketChannel ch) throws Exception {
						ch.config().setConnectTimeoutMillis(options.timeout());

						if (null != sslOptions) {
							SSLEngine ssl = new SSLEngineSupplier(sslOptions, true).get();
							if (log.isDebugEnabled()) {
								log.debug("SSL enabled using keystore {}",
										(null != sslOptions.keystoreFile() ? sslOptions.keystoreFile() : "<DEFAULT>"));
							}
							ch.pipeline().addLast(new SslHandler(ssl));
						}
						if (null != nettyOptions && null != nettyOptions.pipelineConfigurer()) {
							nettyOptions.pipelineConfigurer().accept(ch.pipeline());
						}
						final NettyNetChannel<IN, OUT> netChannel = createChannel(ch);
						notifyNewChannel(netChannel);

						ch.pipeline().addLast(createChannelHandlers(netChannel));

						ch.closeFuture().addListener(new ChannelFutureListener() {
							@Override
							public void operationComplete(ChannelFuture future) throws Exception {
								if (log.isDebugEnabled()) {
									log.debug("CLOSE {}", ch);
								}
								netChannel.notifyClose();
							}
						});
					}
				});

		this.connectionSupplier = new Supplier<ChannelFuture>() {
			@Override
			public ChannelFuture get() {
				if (!closing) {
					return bootstrap.connect(getConnectAddress());
				} else {
					return null;
				}
			}
		};
	}

	@Override
	public Promise<NetChannelStream<IN, OUT>> open() {
		final Promise<NetChannelStream<IN, OUT>> connection
				= Promises.ready(getEnvironment(), getDispatcher());

		openChannel(new ConnectingChannelListener(connection));

		return connection;
	}

	@Override
	public Stream<NetChannelStream<IN, OUT>> open(final Reconnect reconnect) {
		final Broadcaster<NetChannelStream<IN, OUT>> connections
				= Broadcaster.create(getEnvironment(), getDispatcher());

		openChannel(new ReconnectingChannelListener(connectAddress, reconnect, connections));

		return connections;
	}

	@Override
	public Promise<Void> close() {
		final Promise<Void> promise = Promises.ready(getEnvironment(), SynchronousDispatcher.INSTANCE);
		ioGroup.shutdownGracefully().addListener(new FutureListener<Object>() {
			@Override
			public void operationComplete(Future<Object> future) throws Exception {
				if(future.isDone() && future.isSuccess()) {
					promise.onComplete();
				}else{
					promise.onError(future.cause());
				}
			}
		});
		return promise;
	}

	@Override
	protected NettyNetChannel<IN, OUT> createChannel(Object nativeChannel) {
		SocketChannel ch = (SocketChannel) nativeChannel;
		int backlog = getEnvironment().getProperty("reactor.tcp.connectionReactorBacklog", Integer.class, 128);

		return new NettyNetChannel<IN, OUT>(
				getEnvironment(),
				getCodec(),
				new NettyEventLoopDispatcher(ch.eventLoop(), backlog),
				getDispatcher(),
				ch
		);
	}

	protected ChannelHandler[] createChannelHandlers(NettyNetChannel<IN,OUT> conn) {
		NettyNetChannelInboundHandler readHandler = new NettyNetChannelInboundHandler()
				.setNetChannel(conn);
		NettyNetChannelOutboundHandler writeHandler = new NettyNetChannelOutboundHandler();

		return new ChannelHandler[]{readHandler, writeHandler};
	}

	private void openChannel(ChannelFutureListener listener) {
		ChannelFuture channel = connectionSupplier.get();
		if (null != channel && null != listener) {
			channel.addListener(listener);
		}
	}

	private class ConnectingChannelListener implements ChannelFutureListener {
		private final Promise<NetChannelStream<IN, OUT>> connection;

		private ConnectingChannelListener(Promise<NetChannelStream<IN, OUT>> connection) {
			this.connection = connection;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			if (!future.isSuccess()) {
				if (log.isErrorEnabled()) {
					log.error(future.cause().getMessage(), future.cause());
				}
				connection.onError(future.cause());
				return;
			}

			if (log.isInfoEnabled()) {
				log.info("CONNECTED: " + future.channel());
			}

			NettyNetChannelInboundHandler inboundHandler = future.channel()
					.pipeline()
					.get(NettyNetChannelInboundHandler.class);
			final NetChannelStream<IN, OUT> ch = inboundHandler.getNetChannel();

			future.channel().closeFuture().addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if (log.isInfoEnabled()) {
						log.info("CLOSED: " + future.channel());
					}
				}
			});

			future.channel().eventLoop().submit(new Runnable() {
				@Override
				public void run() {
					connection.onNext(ch);
				}
			});
		}
	}

	private class ReconnectingChannelListener implements ChannelFutureListener {

		private final AtomicInteger attempts = new AtomicInteger(0);

		private final Reconnect                        reconnect;
		private final Broadcaster<NetChannelStream<IN, OUT>> connections;

		private volatile InetSocketAddress connectAddress;

		private ReconnectingChannelListener(InetSocketAddress connectAddress,
		                                    Reconnect reconnect,
		                                    Broadcaster<NetChannelStream<IN, OUT>> connections) {
			this.connectAddress = connectAddress;
			this.reconnect = reconnect;
			this.connections = connections;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void operationComplete(final ChannelFuture future) throws Exception {
			if (!future.isSuccess()) {
				int attempt = attempts.incrementAndGet();
				Tuple2<InetSocketAddress, Long> tup = reconnect.reconnect(connectAddress, attempt);
				if (null == tup) {
					// do not attempt a reconnect
					if (log.isErrorEnabled()) {
						log.error("Reconnection to {} failed after {} attempts. Giving up.", connectAddress, attempt - 1);
					}
					future.channel().eventLoop().submit(new Runnable() {
						@Override
						public void run() {
							connections.onError(future.cause());
						}
					});
					return;
				}

				attemptReconnect(tup);
			} else {
				// connected
				if (log.isInfoEnabled()) {
					log.info("CONNECTED: " + future.channel());
				}

				final Channel ioCh = future.channel();
				final ChannelPipeline ioChPipline = ioCh.pipeline();
				final NetChannelStream<IN, OUT> ch = ioChPipline.get(NettyNetChannelInboundHandler.class).getNetChannel();

				ioChPipline.addLast(new ChannelDuplexHandler() {
					@Override
					public void channelInactive(ChannelHandlerContext ctx) throws Exception {
						if (log.isInfoEnabled()) {
							log.info("CLOSED: " + ioCh);
						}

						Tuple2<InetSocketAddress, Long> tup = reconnect.reconnect(connectAddress, attempts.incrementAndGet());
						if (null == tup) {
							// do not attempt a reconnect
							return;
						}
						if (!((NettyNetChannel) ch).isClosing()) {
							attemptReconnect(tup);
						} else {
							closing = true;
						}
						super.channelInactive(ctx);
					}
				});

				ioCh.eventLoop().submit(new Runnable() {
					@Override
					public void run() {
						connections.onNext(ch);
					}
				});
			}
		}

		private void attemptReconnect(Tuple2<InetSocketAddress, Long> tup) {
			connectAddress = tup.getT1();
			bootstrap.remoteAddress(connectAddress);
			long delay = tup.getT2();

			if (log.isInfoEnabled()) {
				log.info("Failed to connect to {}. Attempting reconnect in {}ms.", connectAddress, delay);
			}

			getEnvironment().getTimer()
					.submit(
							new Consumer<Long>() {
								@Override
								public void accept(Long now) {
									openChannel(ReconnectingChannelListener.this);
								}
							},
							delay,
							TimeUnit.MILLISECONDS
					)
					.cancelAfterUse();
		}
	}

}
