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

import java.net.InetSocketAddress;

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
import reactor.Publishers;
import reactor.core.error.Exceptions;
import reactor.fn.timer.Timer;
import reactor.io.buffer.Buffer;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.ReactiveChannelHandler;
import reactor.io.net.config.ServerSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.HttpServer;
import reactor.io.net.impl.netty.NettyChannel;
import reactor.io.net.impl.netty.tcp.NettyTcpServer;

/**
 * A Netty-based {@code HttpServer} implementation
 *
 * @author Stephane Maldini
 * @since 2.1
 */
public class NettyHttpServer extends HttpServer<Buffer, Buffer> {

	private static final Logger log = LoggerFactory.getLogger(NettyHttpServer.class);

	protected NettyTcpServer server;

	protected NettyHttpServer(final Timer timer, final InetSocketAddress listenAddress,
			final ServerSocketOptions options, final SslOptions sslOptions) {

		super(timer);

		this.server =
				new NettyTcpServer(timer, listenAddress, options, sslOptions) {

					@Override
					protected void bindChannel(
							ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> handler,
							SocketChannel nativeChannel) {
						NettyHttpServer.this.bindChannel(handler, nativeChannel);
					}
				};
	}

	@Override
	public InetSocketAddress getListenAddress() {
		return this.server.getListenAddress();
	}

	@Override
	protected Publisher<Void> doStart(
			final ReactiveChannelHandler<Buffer, Buffer, HttpChannel<Buffer, Buffer>> handler) {
		return server.start(new ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>>() {
			@Override
			public Publisher<Void> apply(ReactiveChannel<Buffer, Buffer> ch) {
				NettyHttpChannel request = (NettyHttpChannel) ch;

				if (handler != null) {
					handler.apply(request);
				}

				Iterable<? extends Publisher<Void>> handlers = routeChannel(request);

				//404
				if (handlers == null) {
					if (request.markHeadersAsFlushed()) {
						request.delegate()
						       .writeAndFlush(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND));
					}

					return Publishers.empty();
				}
				try {
					return Publishers.<Void>concat(Publishers.from(handlers));
				}
				catch (Throwable t){
					Exceptions.throwIfFatal(t);
					return Publishers.error(t);
				}
				//500
			}
		});
	}

	@Override
	protected Iterable<? extends Publisher<Void>> routeChannel(HttpChannel<Buffer, Buffer> ch) {
		Iterable<? extends Publisher<Void>> handlers = super.routeChannel(ch);
		if (!handlers.iterator()
		             .hasNext()) {
			return null;
		}
		else {
			return handlers;
		}
	}

	@Override
	protected void onWebsocket(HttpChannel<?, ?> next) {
		ChannelPipeline pipeline = ((SocketChannel) next.delegate()).pipeline();
		pipeline.addLast(pipeline.remove(NettyHttpServerHandler.class)
		                         .withWebsocketSupport(next.uri(), null));

	}

	@Override
	protected final Publisher<Void> doShutdown() {
		return server.shutdown();
	}

	protected void bindChannel(
			ReactiveChannelHandler<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>> handler,
			SocketChannel nativeChannel) {

		NettyChannel netChannel =
				new NettyChannel(getDefaultPrefetchSize(), nativeChannel);

		ChannelPipeline pipeline = nativeChannel.pipeline();

		if (log.isDebugEnabled()) {
			pipeline.addLast(new LoggingHandler(NettyHttpServer.class));
		}

		pipeline.addLast(new HttpServerCodec());

		pipeline.addLast(NettyHttpServerHandler.class.getSimpleName(),
				new NettyHttpServerHandler(handler, netChannel));

	}
}