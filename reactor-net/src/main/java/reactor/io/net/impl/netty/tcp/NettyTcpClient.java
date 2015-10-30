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

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLEngine;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Publishers;
import reactor.core.support.BackpressureUtils;
import reactor.core.support.NamedDaemonThreadFactory;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.fn.timer.Timer;
import reactor.fn.tuple.Tuple;
import reactor.fn.tuple.Tuple2;
import reactor.io.buffer.Buffer;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.Reconnect;
import reactor.io.net.config.ClientSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.impl.netty.NettyChannel;
import reactor.io.net.impl.netty.NettyClientSocketOptions;
import reactor.io.net.impl.netty.internal.NettyNativeDetector;
import reactor.io.net.tcp.TcpClient;
import reactor.io.net.tcp.TcpServer;
import reactor.io.net.tcp.ssl.SSLEngineSupplier;

/**
 * A Netty-based {@code TcpClient}.
 * @author Stephane Maldini
 * @since 2.1
 */
public class NettyTcpClient extends TcpClient<Buffer, Buffer> {

	private static final Logger log = LoggerFactory.getLogger(NettyTcpClient.class);

	private final NettyClientSocketOptions nettyOptions;
	private final Bootstrap                bootstrap;
	private final EventLoopGroup           ioGroup;
	private final Supplier<ChannelFuture>  connectionSupplier;

	private volatile InetSocketAddress connectAddress;

	/**
	 * Creates a new NettyTcpClient that will use the given {@code env} for configuration and the given {@code reactor}
	 * to send events. The number of IO threads used by the client is configured by the environment's {@code
	 * reactor.tcp.ioThreadCount} property. In its absence the number of IO threads will be equal to the {@link
	 * reactor.Processors#DEFAULT_POOL_SIZE number of available processors}. </p> The client will connect to the given
	 * {@code connectAddress}, configuring its socket using the given {@code opts}. The given {@code codec} will be used
	 * for encoding and decoding of data.
	 * @param timer The configuration timer
	 * @param connectAddress The address the client will connect to
	 * @param options The configuration options for the client's socket
	 * @param sslOptions The SSL configuration options for the client's socket
	 */
	public NettyTcpClient(Timer timer,
			InetSocketAddress connectAddress,
			final ClientSocketOptions options,
			final SslOptions sslOptions) {
		super(timer, connectAddress, options, sslOptions);
		this.connectAddress = connectAddress;

		if (options instanceof NettyClientSocketOptions) {
			this.nettyOptions = (NettyClientSocketOptions) options;
		}
		else {
			this.nettyOptions = null;

		}
		if (null != nettyOptions && null != nettyOptions.eventLoopGroup()) {
			this.ioGroup = nettyOptions.eventLoopGroup();
		}
		else {
			int ioThreadCount = TcpServer.DEFAULT_TCP_THREAD_COUNT;
			this.ioGroup =
					NettyNativeDetector.newEventLoopGroup(ioThreadCount, new NamedDaemonThreadFactory("reactor-tcp-io"));
		}

		Bootstrap _bootstrap = new Bootstrap().group(ioGroup)
		                                      .channel(NettyNativeDetector.getChannel(ioGroup))
		                                      .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
		                                      .option(ChannelOption.AUTO_READ, sslOptions != null)
				//.remoteAddress(this.connectAddress)
				;

		if (options != null) {
			_bootstrap = _bootstrap.option(ChannelOption.SO_RCVBUF, options.rcvbuf())
			                       .option(ChannelOption.SO_SNDBUF, options.sndbuf())
			                       .option(ChannelOption.SO_KEEPALIVE, options.keepAlive())
			                       .option(ChannelOption.SO_LINGER, options.linger())
			                       .option(ChannelOption.TCP_NODELAY, options.tcpNoDelay())
			                       .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, options.timeout());
		}

		this.bootstrap = _bootstrap;

		this.connectionSupplier = new Supplier<ChannelFuture>() {
			@Override
			public ChannelFuture get() {
				if (started.get()) {
					return bootstrap.connect(getConnectAddress());
				}
				else {
					return null;
				}
			}
		};
	}

	@Override
	@SuppressWarnings("unchecked")
	protected Publisher<Void> doStart(final ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> handler) {

		final ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> targetHandler =
				null == handler ? (ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>>) PING :
						handler;

		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(final SocketChannel ch) throws Exception {
				bindChannel(targetHandler, ch);
			}
		});

		return new Publisher<Void>() {
			@Override
			public void subscribe(Subscriber<? super Void> s) {
				ChannelFuture channelFuture = connectionSupplier.get();

				if (channelFuture == null) {
					throw new IllegalStateException("Connection supplier didn't return any connection");
				}

				new NettyChannel.FuturePublisher<>(channelFuture).subscribe(s);
			}
		};
	}

	@Override
	protected Publisher<Tuple2<InetSocketAddress, Integer>> doStart(final ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> handler,
			final Reconnect reconnect) {

		bootstrap.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(final SocketChannel ch) throws Exception {
				bindChannel(handler, ch);
			}
		});

		return new ReconnectingChannelPublisher(connectAddress, reconnect);
	}

	@Override
	protected Publisher<Void> doShutdown() {

		if (nettyOptions != null && nettyOptions.eventLoopGroup() != null) {
			return Publishers.empty();
		}

		return new NettyChannel.FuturePublisher<Future<?>>(ioGroup.shutdownGracefully());
	}

	protected void addSecureHandler(SocketChannel ch) throws Exception {
		SSLEngine ssl = new SSLEngineSupplier(getSslOptions(), true).get();
		if (log.isDebugEnabled()) {
			log.debug("SSL enabled using keystore {}", (
					null != getSslOptions() && null != getSslOptions().keystoreFile() ? getSslOptions().keystoreFile() :
							"<DEFAULT>"));
		}
		ch.pipeline()
		  .addLast(new SslHandler(ssl));
	}

	protected void bindChannel(ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> handler,
			SocketChannel ch) throws Exception {

		if (null != getSslOptions()) {
			addSecureHandler(ch);
		}
		else {
			ch.config()
			  .setAutoRead(false);
		}

		NettyChannel netChannel = new NettyChannel(getDefaultPrefetchSize(), ch);

		ChannelPipeline pipeline = ch.pipeline();

		if (null != nettyOptions && null != nettyOptions.pipelineConfigurer()) {
			nettyOptions.pipelineConfigurer()
			            .accept(pipeline);
		}
		if (log.isDebugEnabled()) {
			pipeline.addLast(new LoggingHandler(NettyTcpClient.class));
		}
		pipeline.addLast(new NettyChannelHandlerBridge(handler, netChannel));
	}

	private class ReconnectingChannelPublisher implements Publisher<Tuple2<InetSocketAddress, Integer>> {

		private final AtomicInteger attempts = new AtomicInteger(0);
		private final Reconnect reconnect;

		private volatile InetSocketAddress connectAddress;

		private ReconnectingChannelPublisher(InetSocketAddress connectAddress, Reconnect reconnect) {
			this.connectAddress = connectAddress;
			this.reconnect = reconnect;
		}

		@Override
		public void subscribe(final Subscriber<? super Tuple2<InetSocketAddress, Integer>> s) {
			final ChannelFuture channelOpen = connectionSupplier.get();
			if (null == channelOpen) {
				throw new IllegalStateException("No connection supplied");
			}

			s.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					BackpressureUtils.checkRequest(n);
					channelOpen.addListener(new AfterOpen(s));
				}

				@Override
				public void cancel() {

				}
			});
		}

		private void attemptReconnect(final Subscriber<? super Tuple2<InetSocketAddress, Integer>> subscriber,
				final Tuple2<InetSocketAddress, Long> tup) {

			connectAddress = tup.getT1();
			bootstrap.remoteAddress(connectAddress);
			long delay = tup.getT2();

			if (log.isInfoEnabled()) {
				log.info("Failed to connect to {}. Attempting reconnect in {}ms.", connectAddress, delay);
			}

			getDefaultTimer().submit(new Consumer<Long>() {
				@Override
				public void accept(Long now) {
					final ChannelFuture channelOpen = connectionSupplier.get();
					if (null == channelOpen) {
						throw new IllegalStateException("No connection supplied");
					}
					channelOpen.addListener(new AfterOpen(subscriber));
				}
			}, delay, TimeUnit.MILLISECONDS);
		}

		private class AfterOpen implements ChannelFutureListener {

			private final Subscriber<? super Tuple2<InetSocketAddress, Integer>> s;

			public AfterOpen(Subscriber<? super Tuple2<InetSocketAddress, Integer>> s) {
				this.s = s;
			}

			@SuppressWarnings("unchecked")
			@Override
			public void operationComplete(final ChannelFuture future) throws Exception {
				//FIXME demand
				s.onNext(Tuple.of(connectAddress, attempts.get()));
				if (!future.isSuccess()) {
					int attempt = attempts.incrementAndGet();
					Tuple2<InetSocketAddress, Long> tup = reconnect.reconnect(connectAddress, attempt);
					if (null == tup) {
						// do not attempt a reconnect
						if (log.isErrorEnabled()) {
							log.error("Reconnection to {} failed after {} attempts. Giving up.", connectAddress, attempt - 1);
						}
						future.channel()
						      .eventLoop()
						      .submit(new Runnable() {
							      @Override
							      public void run() {
								      s.onError(future.cause());
							      }
						      });
						return;
					}
					attemptReconnect(s, tup);
				}
				else {
					// connected
					if (log.isInfoEnabled()) {
						log.info("CONNECTED: " + future.channel());
					}
					final Channel ioCh = future.channel();
					ioCh.pipeline()
					    .addLast(new ChannelDuplexHandler() {
						    @Override
						    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
							    if (log.isInfoEnabled()) {
								    log.info("CLOSED: " + ioCh);
							    }

							    Tuple2<InetSocketAddress, Long> tup =
									    reconnect.reconnect(connectAddress, attempts.incrementAndGet());
							    if (null == tup) {
								    s.onComplete();
								    // do not attempt a reconnect
								    return;
							    }

							    attemptReconnect(s, tup);
							    super.channelInactive(ctx);
						    }
					    });

				}
			}

		}
	}

	@Override
	protected boolean shouldFailOnStarted() {
		return false;
	}
}