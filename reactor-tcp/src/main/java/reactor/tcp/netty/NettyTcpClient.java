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
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.Stream;
import reactor.core.composable.spec.Promises;
import reactor.core.composable.spec.Streams;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.io.Buffer;
import reactor.support.NamedDaemonThreadFactory;
import reactor.tcp.Reconnect;
import reactor.tcp.TcpClient;
import reactor.tcp.TcpConnection;
import reactor.tcp.config.ClientSocketOptions;
import reactor.tcp.config.SslOptions;
import reactor.tcp.encoding.Codec;
import reactor.tcp.ssl.SSLEngineSupplier;
import reactor.tuple.Tuple2;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Netty-based {@code TcpClient}.
 *
 * @param <IN>
 * 		The type that will be received by this client
 * @param <OUT>
 * 		The type that will be sent by this client
 *
 * @author Jon Brisbin
 */
public class NettyTcpClient<IN, OUT> extends TcpClient<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(NettyTcpClient.class);

	private final    Bootstrap               bootstrap;
	private final    Reactor                 eventsReactor;
	private final    ClientSocketOptions     options;
	private final    EventLoopGroup          ioGroup;
	private final    Supplier<ChannelFuture> connectionSupplier;
	private volatile InetSocketAddress       connectAddress;
	private volatile boolean                 closing;

	/**
	 * Creates a new NettyTcpClient that will use the given {@code env} for configuration and the given {@code reactor}
	 * to
	 * send events. The number of IO threads used by the client is configured by the environment's {@code
	 * reactor.tcp.ioThreadCount} property. In its absence the number of IO threads will be equal to the {@link
	 * Environment#PROCESSORS number of available processors}. </p> The client will connect to the given {@code
	 * connectAddress}, configuring its socket using the given {@code opts}. The given {@code codec} will be used for
	 * encoding and decoding of data.
	 *
	 * @param env
	 * 		The configuration environment
	 * @param reactor
	 * 		The reactor used to send events
	 * @param connectAddress
	 * 		The address the client will connect to
	 * @param opts
	 * 		The configuration options for the client's socket
	 * @param codec
	 * 		The codec used to encode and decode data
	 */
	public NettyTcpClient(@Nonnull Environment env,
	                      @Nonnull Reactor reactor,
	                      @Nonnull InetSocketAddress connectAddress,
	                      @Nonnull ClientSocketOptions opts,
	                      @Nullable final SslOptions sslOpts,
	                      @Nullable Codec<Buffer, IN, OUT> codec) {
		super(env, reactor, connectAddress, opts, sslOpts, codec);
		this.eventsReactor = reactor;
		this.connectAddress = connectAddress;
		this.options = opts;

		int ioThreadCount = env.getProperty("reactor.tcp.ioThreadCount", Integer.class, Environment.PROCESSORS);
		ioGroup = new NioEventLoopGroup(ioThreadCount, new NamedDaemonThreadFactory("reactor-tcp-io"));

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

						if(null != sslOpts) {
							SSLEngine ssl = new SSLEngineSupplier(sslOpts, true).get();
							log.debug("SSL enabled using keystore {}",
							          (null != sslOpts.keystoreFile() ? sslOpts.keystoreFile() : "<DEFAULT>"));
							ch.pipeline().addLast(new SslHandler(ssl));
						}
						if(options instanceof NettyClientSocketOptions && null != ((NettyClientSocketOptions)options)
								.pipelineConfigurer()) {
							((NettyClientSocketOptions)options).pipelineConfigurer().accept(ch.pipeline());
						}
						ch.pipeline().addLast(createChannelHandlers(ch));
					}
				});

		this.connectionSupplier = new Supplier<ChannelFuture>() {
			@Override
			public ChannelFuture get() {
				if(!closing) {
					return bootstrap.connect(NettyTcpClient.this.connectAddress);
				} else {
					return null;
				}
			}
		};
	}

	@Override
	public Promise<TcpConnection<IN, OUT>> open() {
		final Deferred<TcpConnection<IN, OUT>, Promise<TcpConnection<IN, OUT>>> connection
				= Promises.defer(env, eventsReactor.getDispatcher());

		createConnection(createConnectListener(connection));

		return connection.compose();
	}

	@Override
	public Stream<TcpConnection<IN, OUT>> open(final Reconnect reconnect) {
		final Deferred<TcpConnection<IN, OUT>, Stream<TcpConnection<IN, OUT>>> connections
				= Streams.defer(env, eventsReactor.getDispatcher());
		createConnection(createReconnectListener(connections, reconnect));

		return connections.compose();
	}

	@Override
	protected <C> TcpConnection<IN, OUT> createConnection(C channel) {
		SocketChannel ch = (SocketChannel)channel;
		int backlog = env.getProperty("reactor.tcp.connectionReactorBacklog", Integer.class, 128);

		return new NettyTcpConnection<IN, OUT>(
				env,
				getCodec(),
				new NettyEventLoopDispatcher(ch.eventLoop(), backlog),
				eventsReactor,
				ch,
				connectAddress
		);
	}

	protected ChannelHandler[] createChannelHandlers(SocketChannel ch) {
		NettyTcpConnection<IN, OUT> conn = (NettyTcpConnection<IN, OUT>)select(ch);
		return new ChannelHandler[]{new NettyTcpConnectionChannelInboundHandler(conn)};
	}

	@Override
	@SuppressWarnings({"rawtypes", "unchecked"})
	protected void doClose(final Deferred<Void, Promise<Void>> d) {
		this.closing = true;
		try {
			final CountDownLatch latch = new CountDownLatch(1);
			this.ioGroup.shutdownGracefully().addListener(new GenericFutureListener() {
				@Override
				public void operationComplete(Future future) throws Exception {
					latch.countDown();
				}
			});
			if(latch.await(30, TimeUnit.SECONDS)) {
				d.accept((Void)null);
			} else {
				d.accept(new TimeoutException("NettyTcpClient could not close connection after 30 seconds"));
			}
		} catch(InterruptedException e) {
			Thread.currentThread().interrupt();
			d.accept(e);
		}
	}

	private ChannelFutureListener createConnectListener(final Deferred<TcpConnection<IN, OUT>, Promise<TcpConnection<IN,
			OUT>>> connection) {
		return new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if(!future.isSuccess()) {
					if(log.isErrorEnabled()) {
						log.error(future.cause().getMessage(), future.cause());
					}
					connection.accept(future.cause());
					return;
				}

				if(log.isInfoEnabled()) {
					log.info("CONNECT: " + future.channel());
				}
				final NettyTcpConnection<IN, OUT> conn = (NettyTcpConnection<IN, OUT>)select(future.channel());
				future.channel().closeFuture().addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						if(log.isInfoEnabled()) {
							log.info("CLOSED: " + future.channel());
						}
						NettyTcpClient.this.connections.unregister(future.channel());
						notifyClose(conn);
					}
				});

				connection.accept(conn);
			}
		};
	}

	private ChannelFutureListener createReconnectListener(final Deferred<TcpConnection<IN, OUT>, Stream<TcpConnection<IN, OUT>>> connections,
	                                                      final Reconnect reconnect) {
		return new ChannelFutureListener() {
			final AtomicInteger attempts = new AtomicInteger(0);
			final ChannelFutureListener self = this;

			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if(!future.isSuccess()) {
					int attempt = attempts.incrementAndGet();
					Tuple2<InetSocketAddress, Long> tup = reconnect.reconnect(connectAddress, attempt);
					if(null == tup) {
						// do not attempt a reconnect
						if(log.isErrorEnabled()) {
							log.error("Reconnection to {} failed after {} attempts.", connectAddress, attempt - 1);
						}
						connections.accept(future.cause());
						return;
					}

					connectAddress = tup.getT1();
					bootstrap.remoteAddress(connectAddress);
					long delay = tup.getT2();

					if(log.isInfoEnabled()) {
						log.info("Attempting to reconnect to {} after {}ms", connectAddress, delay);
					}
					env.getRootTimer().submit(
							new Consumer<Long>() {
								@Override
								public void accept(Long now) {
									createConnection(self);
								}
							},
							delay,
							TimeUnit.MILLISECONDS
					);
				} else {
					if(log.isInfoEnabled()) {
						log.info("CONNECT: " + future.channel());
					}
					final NettyTcpConnection<IN, OUT> conn = (NettyTcpConnection<IN, OUT>)select(future.channel());
					future.channel().closeFuture().addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							if(log.isInfoEnabled()) {
								log.info("CLOSED: " + future.channel());
							}
							NettyTcpClient.this.connections.unregister(future.channel());
							notifyClose(conn);
							if(!conn.isClosing()) {
								createConnection(self);
							}
						}
					});
					// hopefully avoid a race condition with user code currently assigning handlers
					// by accepting on the 'next tick' of the event loop
					future.channel().eventLoop().execute(
							new Runnable() {
								@Override
								public void run() {
									connections.accept(conn);
								}
							}
					);
				}
			}
		};
	}

	private void createConnection(ChannelFutureListener listener) {
		ChannelFuture channel = connectionSupplier.get();
		if(channel != null) {
			channel.addListener(listener);
		}
	}

}
