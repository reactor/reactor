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
import reactor.fn.Registration;
import reactor.fn.registry.CachingRegistry;
import reactor.fn.registry.Registry;
import reactor.fn.selector.Selector;
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
	private final Registry<TcpConnection<IN, OUT>> connections = new CachingRegistry<TcpConnection<IN, OUT>>(null);

	private final Codec<Buffer, IN, OUT> codec;
	private final ServerBootstrap        bootstrap;

	protected final Tuple2<Selector, Object> shutdown   = $();
	protected final Tuple2<Selector, Object> connection = $();
	protected final Environment env;
	protected final Reactor     reactor;

	protected TcpServer(Environment env,
											Reactor reactor,
											InetSocketAddress listenAddress,
											int backlog,
											int rcvbuf,
											int sndbuf,
											Codec<Buffer, IN, OUT> codec,
											Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers) {
		Assert.notNull(env, "A TcpServer cannot be created without a properly-configured Environment.");
		Assert.notNull(reactor, "A TcpServer cannot be created without a properly-configured Reactor.");
		this.env = env;
		this.reactor = reactor;
		this.codec = codec;

		for (final Consumer<TcpConnection<IN, OUT>> consumer : connectionConsumers) {
			this.reactor.on(connection.getT1(), new Consumer<Event<TcpConnection<IN, OUT>>>() {
				@Override
				public void accept(Event<TcpConnection<IN, OUT>> ev) {
					consumer.accept(ev.getData());
				}
			});
		}

		int selectThreadCount = env.getProperty("reactor.tcp.selectThreadCount", Integer.class, Environment.PROCESSORS);
		int ioThreadCount = env.getProperty("reactor.tcp.ioThreadCount", Integer.class, Environment.PROCESSORS);
		EventLoopGroup selectorGroup = new NioEventLoopGroup(selectThreadCount, new NamedDaemonThreadFactory("reactor-tcp-select"));
		EventLoopGroup ioGroup = new NioEventLoopGroup(ioThreadCount, new NamedDaemonThreadFactory("reactor-tcp-io"));

		this.bootstrap = new ServerBootstrap()
				.group(selectorGroup, ioGroup)
				.channel(NioServerSocketChannel.class)
				.option(ChannelOption.SO_BACKLOG, backlog)
				.option(ChannelOption.SO_RCVBUF, rcvbuf)
				.option(ChannelOption.SO_SNDBUF, sndbuf)
				.localAddress((null == listenAddress ? new InetSocketAddress(3000) : listenAddress))
				.handler(new LoggingHandler())
				.childHandler(new ChannelInitializer<SocketChannel>() {

					@Override
					public void initChannel(final SocketChannel ch) throws Exception {
						ch.pipeline().addLast(createChannelHandlers(ch));
						ch.closeFuture().addListener(new ChannelFutureListener() {
							@Override
							public void operationComplete(ChannelFuture future) throws Exception {
								for (Registration<? extends TcpConnection<IN, OUT>> reg : connections.select(ch)) {
									reg.getObject().close();
									reg.cancel();
								}
							}
						});
					}

					@Override
					public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
						TcpServer.this.reactor.notify(cause.getClass(), Event.wrap(cause));
					}
				});
	}

	public TcpServer<IN, OUT> start() {
		return start(null);
	}

	public TcpServer<IN, OUT> start(final Consumer<Void> started) {
		ChannelFuture bindFuture = bootstrap.bind();
		if (null != started) {
			bindFuture.addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					Fn.schedule(started, null, reactor);
				}
			});
		}

		return this;
	}

	public TcpServer<IN, OUT> shutdown() {
		bootstrap.shutdown();
		reactor.notify(shutdown.getT2(), Event.NULL_EVENT);
		return this;
	}

	protected Registration<? extends TcpConnection<IN, OUT>> register(SocketChannel ch, TcpConnection<IN, OUT> conn) {
		return connections.register($(ch), conn);
	}

	protected NettyTcpConnection<IN, OUT> registerConnection(SocketChannel ch) {
		NettyTcpConnection<IN, OUT> conn = new NettyTcpConnection<IN, OUT>(
				ch,
				null != codec ? codec.decoder() : null,
				null != codec ? codec.encoder() : null
		);

		register(ch, conn);
		reactor.notify(connection.getT2(), Event.wrap(conn));

		return conn;
	}

	protected ChannelHandler[] createChannelHandlers(SocketChannel ch) {
		final NettyTcpConnection<IN, OUT> conn = registerConnection(ch);

		ChannelHandler readHandler = new ChannelInboundByteHandlerAdapter() {
			@Override
			public void inboundBufferUpdated(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
				int len = in.readableBytes();
				Buffer b = new Buffer(len, true);
				in.readBytes(b.byteBuffer());
				b.flip();
				TcpServer.this.reactor.notify(conn.getReadKey(), Event.wrap(b));
			}
		};

		return new ChannelHandler[]{readHandler};
	}

	protected class NettyTcpConnection<IN, OUT> implements TcpConnection<IN, OUT> {
		private final long                     created = System.currentTimeMillis();
		private final Tuple2<Selector, Object> read    = $();

		private final SocketChannel         channel;
		private final Function<Buffer, IN>  decoder;
		private final Function<OUT, Buffer> encoder;

		public NettyTcpConnection(final SocketChannel channel,
															final Function<Buffer, IN> decoder,
															final Function<OUT, Buffer> encoder) {
			this.channel = channel;
			this.decoder = decoder;
			this.encoder = encoder;
		}

		public Selector getReadSelector() {
			return read.getT1();
		}

		public Object getReadKey() {
			return read.getT2();
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
			reactor.on(read.getT1(), new ReadConsumer<IN>(decoder, consumer));
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

	private static class ReadConsumer<IN> implements Consumer<Event<Buffer>> {
		private final Function<Buffer, IN> decoder;
		private final Consumer<IN>         consumer;

		private ReadConsumer(Function<Buffer, IN> decoder,
												 Consumer<IN> consumer) {
			this.decoder = decoder;
			this.consumer = consumer;
		}

		@SuppressWarnings("unchecked")
		@Override
		public void accept(Event<Buffer> ev) {
			if (null != decoder) {
				IN in;
				while (null != (in = decoder.apply(ev.getData()))) {
					consumer.accept(in);
				}
			} else {
				consumer.accept((IN) ev.getData());
			}
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
		private int               rcvbuf        = 8 * 1024;
		private int               sndbuf        = 8 * 1024;
		private Collection<Consumer<TcpConnection<IN, OUT>>> connectionConsumers;
		private Codec<Buffer, IN, OUT>                       codec;

		public Spec<IN, OUT> backlog(int backlog) {
			this.backlog = backlog;
			return this;
		}

		public Spec<IN, OUT> bufferSize(int rcvbuf, int sndbuf) {
			this.rcvbuf = rcvbuf;
			this.sndbuf = sndbuf;
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
					rcvbuf,
					sndbuf,
					codec,
					connectionConsumers
			);
		}
	}

}
