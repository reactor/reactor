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
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.ChannelStream;
import reactor.io.net.ReactorChannelHandler;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.HttpServer;
import reactor.io.net.impl.netty.NettyChannelStream;
import reactor.io.net.impl.netty.tcp.NettyTcpServer;
import reactor.io.net.tcp.TcpServer;
import reactor.rx.Promise;
import reactor.rx.Streams;

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

	private static final Logger log = LoggerFactory.getLogger(NettyHttpServer.class);

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
			protected void bindChannel(ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler, Object nativeChannel) {
				NettyHttpServer.this.bindChannel(handler, nativeChannel);
			}
		};
	}

	@Override
	public InetSocketAddress getListenAddress() {
		return this.server.getListenAddress();
	}

	@Override
	protected Promise<Void> doStart(final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler){
		return server.start(new ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>>() {
			@Override
			public Publisher<Void> apply(ChannelStream<IN, OUT> ch) {
				NettyHttpChannel<IN, OUT> request = (NettyHttpChannel<IN, OUT>)ch;

				if(handler != null){
					handler.apply(request);
				}

				Iterable<? extends Publisher<Void>> handlers = routeChannel(request);

				//404
				if(handlers == null){
					request.delegate().writeAndFlush(
							new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND));

					return Streams.empty();
				}
				return Streams.merge(handlers);
			}
		});
	}

	@Override
	protected Iterable<? extends Publisher<Void>> routeChannel(HttpChannel<IN, OUT> ch) {
		Iterable<? extends Publisher<Void>> handlers = super.routeChannel(ch);
		if (!handlers.iterator().hasNext()) {
			return null;
		} else {
			return handlers;
		}
	}

	@Override
	protected final Promise<Void> doShutdown() {
		return server.shutdown();
	}

	protected void bindChannel(ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler, Object nativeChannel) {
		SocketChannel ch = (SocketChannel) nativeChannel;

		NettyChannelStream<IN, OUT> netChannel = new NettyChannelStream<IN, OUT>(
				getDefaultEnvironment(),
				getDefaultCodec(),
				getDefaultPrefetchSize(),
				getDefaultDispatcher(),
				ch
		);


		ChannelPipeline pipeline = ch.pipeline();

		if(log.isDebugEnabled()){
			pipeline.addLast(new LoggingHandler(NettyHttpServer.class));
		}

		pipeline
				.addLast(new HttpServerCodec())
				.addLast(new NettyHttpServerHandler<IN, OUT>(handler, netChannel));

	}
}
