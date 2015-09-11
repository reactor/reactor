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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.fn.timer.Timer;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.ChannelStream;
import reactor.io.net.ReactorChannelHandler;
import reactor.io.net.Reconnect;
import reactor.io.net.config.ClientSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.impl.netty.NettyChannelHandlerBridge;
import reactor.io.net.impl.netty.NettyChannelStream;
import reactor.io.net.impl.netty.NettyClientSocketOptions;
import reactor.io.net.tcp.TcpClient;
import reactor.io.net.tcp.TcpServer;
import reactor.io.net.tcp.ssl.SSLEngineSupplier;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Stream;
import reactor.rx.broadcast.BehaviorBroadcaster;
import reactor.rx.broadcast.Broadcaster;

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

	private static final Logger log = LoggerFactory.getLogger(NettyTcpClient.class);

	private final NettyClientSocketOptions nettyOptions;
	private final Bootstrap                bootstrap;
	private final EventLoopGroup           ioGroup;
	private final Supplier<ChannelFuture>  connectionSupplier;

	private volatile InetSocketAddress connectAddress;

	/**
	 * Creates a new NettyTcpClient that will use the given {@code env} for configuration and the given {@code
	 * reactor} to
	 * send events. The number of IO threads used by the client is configured by the environment's {@code
	 * reactor.tcp.ioThreadCount} property. In its absence the number of IO threads will be equal to the {@link
	 * reactor.Processors#DEFAULT_POOL_SIZE number of available processors}. </p> The client will connect to the given {@code
	 * connectAddress}, configuring its socket using the given {@code opts}. The given {@code codec} will be used for
	 * encoding and decoding of data.
	 *
	 * @param timer            The configuration timer
	 * @param connectAddress The address the client will connect to
	 * @param options        The configuration options for the client's socket
	 * @param sslOptions     The SSL configuration options for the client's socket
	 * @param codec          The codec used to encode and decode data
	 */
	public NettyTcpClient(Timer timer,
	                      InetSocketAddress connectAddress,
	                      final ClientSocketOptions options,
	                      final SslOptions sslOptions,
	                      Codec<Buffer, IN, OUT> codec) {
		super(timer, connectAddress, options, sslOptions, codec);
		this.connectAddress = connectAddress;

		if (options instanceof NettyClientSocketOptions) {
			this.nettyOptions = (NettyClientSocketOptions) options;
		} else {
			this.nettyOptions = null;

		}

		Bootstrap _bootstrap = new Bootstrap()
				.channel(NioSocketChannel.class)
				.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
				.option(ChannelOption.AUTO_READ, sslOptions != null)
						//.remoteAddress(this.connectAddress)
				;

		if (options != null) {
			_bootstrap = _bootstrap.option(ChannelOption.SO_RCVBUF, options.rcvbuf())
			  .option(ChannelOption.SO_SNDBUF, options.sndbuf())
			  .option(ChannelOption.SO_KEEPALIVE, options.keepAlive())
			  .option(ChannelOption.SO_LINGER, options.linger())
			  .option(ChannelOption.TCP_NODELAY, options.tcpNoDelay());
		}

		if (null != nettyOptions && null != nettyOptions.eventLoopGroup()) {
			this.ioGroup = nettyOptions.eventLoopGroup();
		} else {
			int ioThreadCount = TcpServer.DEFAULT_TCP_THREAD_COUNT;
			this.ioGroup = new NioEventLoopGroup(ioThreadCount, new NamedDaemonThreadFactory("reactor-tcp-io"));
		}

		this.bootstrap = _bootstrap.group(ioGroup);

		this.connectionSupplier = new Supplier<ChannelFuture>() {
			@Override
			public ChannelFuture get() {
				if (started.get()) {
					return bootstrap.connect(getConnectAddress());
				} else {
					return null;
				}
			}
		};
	}

	@Override
	protected Promise<Void> doStart(final ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler) {

		final Promise<Void> promise = Promises.ready();

		ChannelFutureListener listener = new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if(future.isSuccess()){
					promise.onComplete();
				}else{
					promise.onError(future.cause());
				}
			}
		};
		addHandler(handler);
		openChannel(listener);
		return promise;
	}

	@SuppressWarnings("unchecked")
	private void addHandler(ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler){
		final ReactorChannelHandler<IN,OUT, ChannelStream<IN, OUT>> targetHandler = null == handler ?
				(ReactorChannelHandler<IN,OUT, ChannelStream<IN, OUT>> )PING :
				handler;

		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(final SocketChannel ch) throws Exception {
				if(getOptions() != null) {
					ch.config().setConnectTimeoutMillis(getOptions().timeout());
				}

				if (null != getSslOptions()) {
					addSecureHandler(ch);
				} else {
					ch.config().setAutoRead(false);
				}

				if (null != nettyOptions && null != nettyOptions.pipelineConfigurer()) {
					nettyOptions.pipelineConfigurer().accept(ch.pipeline());
				}

				bindChannel(targetHandler, ch);
			}
		});
	}

	protected void addSecureHandler(SocketChannel ch) throws Exception {
		SSLEngine ssl = new SSLEngineSupplier(getSslOptions(), true).get();
		if (log.isDebugEnabled()) {
			log.debug("SSL enabled using keystore {}",
					(null != getSslOptions() && null != getSslOptions().keystoreFile() ? getSslOptions().keystoreFile() : "<DEFAULT>"));
		}
		ch.pipeline().addLast(new SslHandler(ssl));
	}

	@Override
	protected Stream<Tuple2<InetSocketAddress, Integer>> doStart(ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>>
			                                                       handler, final Reconnect reconnect) {

		ReconnectingChannelListener listener = new ReconnectingChannelListener(connectAddress, reconnect);
		addHandler(handler);
		openChannel(listener);
		return listener.broadcaster;
	}

	@Override
	protected Promise<Void> doShutdown() {

		if(nettyOptions != null && nettyOptions.eventLoopGroup() != null){
			return Promises.success();
		}

		final Promise<Void> promise = Promises.ready();

		ioGroup.shutdownGracefully().addListener(new FutureListener<Object>() {
			@Override
			public void operationComplete(Future<Object> future) throws Exception {
				if (future.isDone() && future.isSuccess()) {
					promise.onComplete();
				} else {
					promise.onError(future.cause());
				}
			}
		});
		return promise;
	}

	protected void bindChannel(ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler, SocketChannel nativeChannel) {

		NettyChannelStream<IN, OUT> netChannel = new NettyChannelStream<IN, OUT>(
				getDefaultTimer(),
				getDefaultCodec(),
				getDefaultPrefetchSize(),
				nativeChannel
		);

		ChannelPipeline pipeline = nativeChannel.pipeline();
		if (log.isDebugEnabled()) {
			pipeline.addLast(new LoggingHandler(NettyTcpClient.class));
		}
		pipeline.addLast(
				new NettyChannelHandlerBridge<IN, OUT>(handler, netChannel)
		);
	}

	private void openChannel(ChannelFutureListener listener) {
		ChannelFuture channel = connectionSupplier.get();
		if (null != channel && null != listener) {
			channel.addListener(listener);
		}
	}

	private class ReconnectingChannelListener implements ChannelFutureListener {

		private final AtomicInteger attempts = new AtomicInteger(0);
		private final Reconnect reconnect;
		private final Broadcaster<Tuple2<InetSocketAddress, Integer>> broadcaster =
				BehaviorBroadcaster.create(getDefaultTimer());

		private volatile InetSocketAddress connectAddress;

		private ReconnectingChannelListener(InetSocketAddress connectAddress,
		                                    Reconnect reconnect) {
			this.connectAddress = connectAddress;
			this.reconnect = reconnect;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void operationComplete(final ChannelFuture future) throws Exception {
			broadcaster.onNext(Tuple.of(connectAddress, attempts.get()));
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
							broadcaster.onError(future.cause());
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
				ioCh.pipeline().addLast(new ChannelDuplexHandler() {
					@Override
					public void channelInactive(ChannelHandlerContext ctx) throws Exception {
						if (log.isInfoEnabled()) {
							log.info("CLOSED: " + ioCh);
						}

						Tuple2<InetSocketAddress, Long> tup = reconnect.reconnect(connectAddress, attempts.incrementAndGet());
						if (null == tup) {
							broadcaster.onComplete();
							// do not attempt a reconnect
							return;
						}

						attemptReconnect(tup);
						super.channelInactive(ctx);
					}
				});
			}
		}

		private void attemptReconnect(final Tuple2<InetSocketAddress, Long> tup) {
			connectAddress = tup.getT1();
			bootstrap.remoteAddress(connectAddress);
			long delay = tup.getT2();

			if (log.isInfoEnabled()) {
				log.info("Failed to connect to {}. Attempting reconnect in {}ms.", connectAddress, delay);
			}

			getDefaultTimer()
					.submit(
							new Consumer<Long>() {
								@Override
								public void accept(Long now) {
									openChannel(ReconnectingChannelListener.this);
								}
							},
							delay,
							TimeUnit.MILLISECONDS
					);
		}
	}

}
