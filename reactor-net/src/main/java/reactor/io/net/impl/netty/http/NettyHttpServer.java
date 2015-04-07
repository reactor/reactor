/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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

package reactor.io.net.impl.netty.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.logging.LoggingHandler;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.HttpServer;
import reactor.io.net.impl.netty.NettyChannelStream;
import reactor.io.net.impl.netty.NettyEventLoopDispatcher;
import reactor.io.net.impl.netty.tcp.NettyTcpServer;
import reactor.io.net.tcp.TcpServer;
import reactor.rx.Promise;
import reactor.rx.Stream;
import reactor.rx.Streams;
import reactor.rx.action.Control;

import java.net.InetSocketAddress;

/**
 * A Netty-based {@code TcpServer} implementation
 *
 * @param <IN>  The type that will be received by this server
 * @param <OUT> The type that will be sent by this server
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyHttpServer<IN, OUT> extends HttpServer<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(NettyHttpServer.class);

	protected final TcpServer<IN, OUT>                 server;

	protected NettyHttpServer(final Environment env,
	                          final Dispatcher dispatcher,
	                          final InetSocketAddress listenAddress,
	                          final ServerSocketOptions options,
	                          final SslOptions sslOptions,
	                          final Codec<Buffer, IN, OUT> codec) {

		super(env, dispatcher, codec);

		this.server = new NettyTcpServer<IN, OUT>(env,
				dispatcher,
				listenAddress,
				options,
				sslOptions,
				codec){

			@Override
			protected NettyChannelStream<IN, OUT> bindChannel(Object nativeChannel, long prefetch) {
				NettyHttpServer.this.bindChannel(nativeChannel, prefetch);
				return null;
			}
		};

		this.server.consume(null, new Consumer<Throwable>() {
			@Override
			public void accept(Throwable throwable) {
				notifyError(throwable);
			}
		}, new Consumer<Void>() {
			@Override
			public void accept(Void aVoid) {
				notifyShutdown();
			}
		});

	}

	@Override
	public final Promise<Boolean> start(){
		return server.start();
	}

	@Override
	public final Promise<Boolean> shutdown() {
		return server.shutdown();
	}

	protected HttpChannel<IN, OUT> createServerRequest(NettyChannelStream<IN, OUT> channelStream, io.netty.handler.codec
			.http.HttpRequest content) {
		NettyHttpChannel<IN, OUT> request = new NettyHttpChannel<IN, OUT>(channelStream, server, content, getDefaultCodec());
		Iterable<? extends Publisher<? extends OUT>> handlers = routeChannel(request);

		Stream<OUT> writeStream;
		if (!handlers.iterator().hasNext()) {
			channelStream.write(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND), null, false);
		} else {
			writeStream = Streams.concat(handlers);
			final Control c = subscribeChannelHandlers(writeStream, request);

			channelStream.delegate().closeFuture().addListener(new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					c.cancel();
				}
			});
			channelStream.subscribe(request.in());
		}
		return request;
	}

	@Override
	protected Consumer<Void> completeConsumer(final HttpChannel<IN, OUT> ch) {
		return new Consumer<Void>() {
			@Override
			public void accept(Void aVoid) {
				((Channel)ch.delegate()).writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
			}
		};
	}

	@Override
	protected Control mergeWrite(HttpChannel<IN, OUT> ch) {
		final Control c = super.mergeWrite(ch);
		if(c == null) return null;
		((Channel)ch.delegate()).closeFuture().addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				c.cancel();
			}
		});
		return c;
	}

	@Override
	protected HttpChannel<IN, OUT> bindChannel(Object nativeChannel, long prefetch) {
		SocketChannel ch = (SocketChannel) nativeChannel;
		//int backlog = getEnvironment().getProperty("reactor.tcp.connectionReactorBacklog", Integer.class, 128);

		NettyChannelStream<IN, OUT> netChannel = new NettyChannelStream<IN, OUT>(
				getEnvironment(),
				getDefaultCodec(),
				prefetch == -1l ? getPrefetchSize() : prefetch,
				server,
				new NettyEventLoopDispatcher(ch.eventLoop(), 256),
				getDispatcher(),
				ch
		);


		ChannelPipeline pipeline = ch.pipeline();

		if(log.isDebugEnabled()){
			pipeline.addLast(new LoggingHandler(getClass()));
		}

		pipeline
				.addLast(new HttpServerCodec())
				.addLast(new NettyHttpServerHandler<IN, OUT>(netChannel, this));

		return null;
	}
}
