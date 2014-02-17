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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
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
import reactor.io.encoding.Codec;
import reactor.net.NetChannel;
import reactor.net.Reconnect;
import reactor.net.config.ClientSocketOptions;
import reactor.net.config.SslOptions;
import reactor.net.netty.NettyClientSocketOptions;
import reactor.net.netty.NettyEventLoopDispatcher;
import reactor.net.netty.NettyNetChannel;
import reactor.net.netty.NettyNetChannelInboundHandler;
import reactor.net.tcp.TcpClient;
import reactor.net.tcp.ssl.SSLEngineSupplier;
import reactor.support.NamedDaemonThreadFactory;
import reactor.tuple.Tuple2;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
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
 * @author Stephane Maldini
 */
public class NettyTcpClient<IN, OUT> extends TcpClient<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(NettyTcpClient.class);

	private final Bootstrap               bootstrap;
	private final EventLoopGroup          ioGroup;
	private final Supplier<ChannelFuture> connectionSupplier;

	private volatile InetSocketAddress connectAddress;
	private volatile boolean           closing;

	private volatile SocketChannel channel;

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
	 * @param options
	 * 		The configuration options for the client's socket
	 * @param sslOptions
	 * 		The SSL configuration options for the client's socket
	 * @param codec
	 * 		The codec used to encode and decode data
	 * @param consumers
	 * 		The consumers that will interact with the connection
	 */
	public NettyTcpClient(@Nonnull Environment env,
	                      @Nonnull Reactor reactor,
	                      @Nonnull InetSocketAddress connectAddress,
	                      @Nonnull final ClientSocketOptions options,
	                      @Nullable final SslOptions sslOptions,
	                      @Nullable Codec<Buffer, IN, OUT> codec,
	                      @Nonnull Collection<Consumer<NetChannel<IN, OUT>>> consumers) {
		super(env, reactor, connectAddress, options, sslOptions, codec, consumers);

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

						if(null != sslOptions) {
							SSLEngine ssl = new SSLEngineSupplier(sslOptions, true).get();
							log.debug("SSL enabled using keystore {}",
							          (null != sslOptions.keystoreFile() ? sslOptions.keystoreFile() : "<DEFAULT>"));
							ch.pipeline().addLast(new SslHandler(ssl));
						}
						if(options instanceof NettyClientSocketOptions && null != ((NettyClientSocketOptions)options)
								.pipelineConfigurer()) {
							((NettyClientSocketOptions)options).pipelineConfigurer().accept(ch.pipeline());
						}
						ch.pipeline().addLast(createChannelHandlers(ch));

						NettyTcpClient.this.channel = ch;
					}
				});

		this.connectionSupplier = new Supplier<ChannelFuture>() {
			@Override
			public ChannelFuture get() {
				if(!closing) {
					return bootstrap.connect(getConnectAddress());
				} else {
					return null;
				}
			}
		};
	}

	@Override
	public Promise<NetChannel<IN, OUT>> open() {
		final Deferred<NetChannel<IN, OUT>, Promise<NetChannel<IN, OUT>>> connection
				= Promises.defer(getEnvironment(), getReactor().getDispatcher());

		createConnection(createConnectListener(connection));

		return connection.compose();
	}

	@Override
	public Stream<NetChannel<IN, OUT>> open(final Reconnect reconnect) {
		final Deferred<NetChannel<IN, OUT>, Stream<NetChannel<IN, OUT>>> connections
				= Streams.defer(getEnvironment(), getReactor().getDispatcher());

		createConnection(createReconnectListener(connections, reconnect));

		return connections.compose();
	}

	@Override
	protected <C> NetChannel<IN, OUT> createChannel(C ioChannel) {
		SocketChannel ch = (SocketChannel)ioChannel;
		int backlog = getEnvironment().getProperty("reactor.tcp.connectionReactorBacklog", Integer.class, 128);

		return new NettyNetChannel<IN, OUT>(
				getEnvironment(),
				getCodec(),
				new NettyEventLoopDispatcher(ch.eventLoop(), backlog),
				getReactor(),
				ch
		);
	}

	protected ChannelHandler[] createChannelHandlers(SocketChannel ioChannel) {
		NettyNetChannel<IN, OUT> conn = (NettyNetChannel<IN, OUT>)select(ioChannel);
		return new ChannelHandler[]{new NettyNetChannelInboundHandler().setNetChannel(conn)};
	}

	private ChannelFutureListener createConnectListener(final Deferred<NetChannel<IN, OUT>, Promise<NetChannel<IN,
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
				final NettyNetChannel<IN, OUT> conn = (NettyNetChannel<IN, OUT>)select(future.channel());
				future.channel().closeFuture().addListener(new ChannelFutureListener() {
					@Override
					public void operationComplete(ChannelFuture future) throws Exception {
						if(log.isInfoEnabled()) {
							log.info("CLOSED: " + future.channel());
						}
						notifyClose(conn);
						getChannels().unregister(future.channel());
					}
				});

				connection.accept(conn);
			}
		};
	}

	private ChannelFutureListener createReconnectListener(final Deferred<NetChannel<IN, OUT>,
			Stream<NetChannel<IN, OUT>>> connections,
	                                                      final Reconnect reconnect) {
		return new ChannelFutureListener() {
			final AtomicInteger attempts = new AtomicInteger(0);
			final ChannelFutureListener self = this;

			@Override
			public void operationComplete(final ChannelFuture future) throws Exception {
				if(!future.isSuccess()) {
					int attempt = attempts.incrementAndGet();
					Tuple2<InetSocketAddress, Long> tup = reconnect.reconnect(connectAddress, attempt);
					if(null == tup) {
						// do not attempt a reconnect
						if(log.isErrorEnabled()) {
							log.error("Reconnection to {} failed after {} attempts.", connectAddress, attempt - 1);
						}
						// hopefully avoid a race condition with user code currently assigning handlers
						// by accepting on the 'next tick' of the event loop
						future.channel().eventLoop().execute(
								new Runnable() {
									@Override
									public void run() {
										connections.accept(future.cause());
									}
								}
						);
						return;
					}

					connectAddress = tup.getT1();
					bootstrap.remoteAddress(connectAddress);
					long delay = tup.getT2();

					if(log.isInfoEnabled()) {
						log.info("Attempting to reconnect to {} after {}ms", connectAddress, delay);
					}
					getEnvironment().getRootTimer().submit(
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
					final NettyNetChannel<IN, OUT> conn = (NettyNetChannel<IN, OUT>)select(future.channel());
					future.channel().closeFuture().addListener(new ChannelFutureListener() {
						@Override
						public void operationComplete(ChannelFuture future) throws Exception {
							if(log.isInfoEnabled()) {
								log.info("CLOSED: " + future.channel());
							}
							notifyClose(conn);
							if(!conn.isClosing()) {
								int attempt = attempts.incrementAndGet();
								Tuple2<InetSocketAddress, Long> tup = reconnect.reconnect(connectAddress, attempt);
								if(null == tup) {
									// do not attempt a reconnect
								} else {
									createConnection(self);
								}
							}
							getChannels().unregister(future.channel());
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
