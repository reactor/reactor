package reactor.tcp;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Fn;
import reactor.core.ComponentSpec;
import reactor.core.Composable;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.Function;
import reactor.fn.selector.Selector;
import reactor.fn.support.CachingRegistry;
import reactor.fn.support.Registry;
import reactor.fn.tuples.Tuple2;
import reactor.io.Buffer;
import reactor.support.NamedDaemonThreadFactory;
import reactor.tcp.encoding.Codec;
import reactor.util.Assert;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Collections;

import static reactor.Fn.$;

/**
 * @author Jon Brisbin
 */
public class TcpServer<IN, OUT> {

	private final Event<TcpServer<IN, OUT>>        selfEvent   = new Event<TcpServer<IN, OUT>>(this);
	private final Tuple2<Selector, Object>         start       = $();
	private final Tuple2<Selector, Object>         connection  = $();
	private final Registry<TcpConnection<IN, OUT>> connections = new CachingRegistry<TcpConnection<IN, OUT>>(null, null);

	private final Environment            env;
	private final Reactor                reactor;
	private final InetSocketAddress      listenAddress;
	private final Codec<Buffer, IN, OUT> codec;

	private final EventLoopGroup  selectorGroup;
	private final EventLoopGroup  ioGroup;
	private final ServerBootstrap bootstrap;

	private TcpServer(Environment env,
										Reactor reactor,
										InetSocketAddress listenAddress,
										int backlog,
										Codec<Buffer, IN, OUT> codec,
										Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers) {
		Assert.notNull(env, "A TcpServer cannot be created without a properly-configured Environment.");
		Assert.notNull(reactor, "A TcpServer cannot be created without a properly-configured Reactor.");
		this.env = env;
		this.reactor = reactor;
		this.listenAddress = (null == listenAddress ? new InetSocketAddress(3000) : listenAddress);
		this.codec = codec;

		for (final Consumer<TcpConnection<IN, OUT>> consumer : connectionConsumers) {
			this.reactor.on(connection.getT1(), new Consumer<Event<TcpConnection<IN, OUT>>>() {
				@Override
				public void accept(Event<TcpConnection<IN, OUT>> ev) {
					consumer.accept(ev.getData());
				}
			});
		}

		int selectThreads = env.getProperty("reactor.tcp.selectThreadCount", Integer.class, Environment.PROCESSORS);
		int ioThreads = env.getProperty("reactor.tcp.ioThreadCount", Integer.class, Environment.PROCESSORS);
		this.selectorGroup = new NioEventLoopGroup(selectThreads, new NamedDaemonThreadFactory("reactor-tcp-select"));
		this.ioGroup = new NioEventLoopGroup(ioThreads, new NamedDaemonThreadFactory("reactor-tcp-io"));

		this.bootstrap = new ServerBootstrap()
				.group(selectorGroup, ioGroup)
				.channel(NioServerSocketChannel.class)
				.option(ChannelOption.SO_BACKLOG, backlog)
				.localAddress(listenAddress)
				.handler(new LoggingHandler())
				.childHandler(new ChannelInitializer<SocketChannel>() {

					@Override
					public void initChannel(final SocketChannel ch) throws Exception {
						ch.pipeline().addLast(createChannelHandlers(ch));
						ch.closeFuture().addListener(new ChannelFutureListener() {
							@Override
							public void operationComplete(ChannelFuture future) throws Exception {
								System.out.println("channel close: " + ch);
								System.out.println("conn: " + connections.select(ch));
							}
						});
					}

					@Override
					public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
						TcpServer.this.reactor.notify(cause.getClass(), Fn.event(cause));
					}
				});
	}

	public TcpServer<IN, OUT> start() {
		return start(null);
	}

	public TcpServer<IN, OUT> start(final Consumer<TcpServer<IN, OUT>> startConsumer) {
		if (null != startConsumer) {
			reactor.on(start.getT1(), new Consumer<Event<TcpServer<IN, OUT>>>() {
				@Override
				public void accept(Event<TcpServer<IN, OUT>> ev) {
					startConsumer.accept(ev.getData());
				}
			}).cancelAfterUse();
		}

		bootstrap.bind().addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				reactor.notify(start, selfEvent);
			}
		});

		return this;
	}

	protected ChannelHandler[] createChannelHandlers(SocketChannel ch) {
		final NettyTcpConnection<IN, OUT> conn = new NettyTcpConnection<IN, OUT>(
				ch,
				null != codec ? codec.decoder() : null,
				null != codec ? codec.encoder() : null
		);
		connections.register($(ch), conn);

		reactor.notify(connection.getT2(), Fn.event(conn));

		ChannelHandler readHandler = new ChannelInboundByteHandlerAdapter() {
			@Override
			public void inboundBufferUpdated(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
				TcpServer.this.reactor.notify(conn.read.getT2(), Fn.event(new Buffer(in.nioBuffer())));
			}
		};

		return new ChannelHandler[]{readHandler};
	}

	private class NettyTcpConnection<IN, OUT> implements TcpConnection<IN, OUT> {
		final long                     created = System.currentTimeMillis();
		final Tuple2<Selector, Object> read    = $();

		private final SocketChannel         channel;
		private final Function<Buffer, IN>  decoder;
		private final Function<OUT, Buffer> encoder;

		private NettyTcpConnection(final SocketChannel channel,
															 final Function<Buffer, IN> decoder,
															 final Function<OUT, Buffer> encoder) {
			this.channel = channel;
			this.decoder = decoder;
			this.encoder = encoder;
		}

		@Override
		public void close() {
			reactor.getConsumerRegistry().unregister(read.getT2());
		}

		@Override
		public boolean consumable() {
			return !channel.isInputShutdown();
		}

		@Override
		public boolean writable() {
			return !channel.isOutputShutdown();
		}

		@Override
		public TcpConnection<IN, OUT> consume(final Consumer<IN> consumer) {
			reactor.on(read.getT1(), new Consumer<Event<Buffer>>() {
				@SuppressWarnings("unchecked")
				@Override
				public void accept(Event<Buffer> event) {
					if (null != decoder) {
						IN in;
						while (null != (in = decoder.apply(event.getData()))) {
							consumer.accept(in);
						}
					} else {
						consumer.accept((IN) event.getData());
					}
				}
			});
			return this;
		}

		@Override
		public TcpConnection<IN, OUT> send(OUT data, final Consumer<Boolean> onComplete) {
			Fn.schedule(
					new Consumer<OUT>() {
						@Override
						public void accept(OUT data) {
							ChannelFuture writeFuture = null;
							if (null != encoder) {
								Buffer bytes;
								while (null != (bytes = encoder.apply(data)) && bytes.remaining() > 0) {
									writeFuture = channel.write(bytes.asBytes());
								}
							} else {
								writeFuture = channel.write(data);
							}
							if (null != onComplete && null != writeFuture) {
								writeFuture.addListener(new ChannelFutureListener() {
									@Override
									public void operationComplete(ChannelFuture future) throws Exception {
										onComplete.accept(future.isSuccess());
									}
								});
							}
						}
					},
					data,
					reactor
			);
			return this;
		}

		@Override
		public TcpConnection<IN, OUT> send(Composable<OUT> data) {
			data.consume(new Consumer<OUT>() {
				@Override
				public void accept(OUT data) {
					send(data, null);
				}
			});
			return this;
		}
	}

	private static class LoggingHandler extends ChannelHandlerAdapter {
		private final Logger LOG = LoggerFactory.getLogger(TcpServer.class);

		@Override
		public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelFuture future) throws Exception {
			if (LOG.isInfoEnabled()) {
				LOG.info("BIND {}", localAddress);
			}
			super.bind(ctx, localAddress, future);
		}

		@Override
		public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelFuture future) throws Exception {
			if (LOG.isDebugEnabled()) {
				LOG.debug("CONNECT {}", remoteAddress);
			}
			super.connect(ctx, remoteAddress, localAddress, future);
		}
	}

	public static class Spec<IN, OUT> extends ComponentSpec<Spec<IN, OUT>, TcpServer<IN, OUT>> {
		private InetSocketAddress listenAddress = new InetSocketAddress(3000);
		private int               backlog       = 512;
		private Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers;
		private Codec<Buffer, IN, OUT>                       codec;

		public Spec<IN, OUT> backlog(int backlog) {
			this.backlog = backlog;
			return this;
		}

		public Spec<IN, OUT> listen(int port) {
			this.listenAddress = new InetSocketAddress(port);
			return this;
		}

		public Spec<IN, OUT> listen(String host, int port) {
			this.listenAddress = new InetSocketAddress(host, port);
			return this;
		}

		public Spec<IN, OUT> codec(Codec<Buffer, IN, OUT> codec) {
			Assert.isNull(this.codec, "Codec has already been set.");
			this.codec = codec;
			return this;
		}

		public Spec<IN, OUT> consume(Consumer<TcpConnection<IN, OUT>> connectionConsumer) {
			return consume(Collections.singletonList(connectionConsumer));
		}

		public Spec<IN, OUT> consume(Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers) {
			this.connectionConsumers = connectionConsumers;
			return this;
		}

		@Override
		protected TcpServer<IN, OUT> configure(Reactor reactor) {
			return new TcpServer<IN, OUT>(
					env,
					reactor,
					listenAddress,
					backlog,
					codec,
					connectionConsumers
			);
		}
	}

}
