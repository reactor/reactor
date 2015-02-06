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
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LoggingHandler;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.Reconnect;
import reactor.io.net.config.ClientSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.HttpClient;
import reactor.io.net.http.model.Method;
import reactor.io.net.impl.netty.NettyChannelStream;
import reactor.io.net.impl.netty.NettyEventLoopDispatcher;
import reactor.io.net.impl.netty.tcp.NettyTcpClient;
import reactor.rx.Promise;
import reactor.rx.Stream;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;

/**
 * A Netty-based {@code TcpClient}.
 *
 * @param <IN>  The type that will be received by this client
 * @param <OUT> The type that will be sent by this client
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyHttpClient<IN, OUT> extends HttpClient<IN, OUT> {

	private final Logger log = LoggerFactory.getLogger(NettyHttpClient.class);

	private final NettyTcpClient<IN, OUT> client;
	private String lastURL = "http://localhost:8080";

	/**
	 * Creates a new NettyTcpClient that will use the given {@code env} for configuration and the given {@code
	 * reactor} to
	 * send events. The number of IO threads used by the client is configured by the environment's {@code
	 * reactor.tcp.ioThreadCount} property. In its absence the number of IO threads will be equal to the {@link
	 * reactor.Environment#PROCESSORS number of available processors}. </p> The client will connect to the given {@code
	 * connectAddress}, configuring its socket using the given {@code opts}. The given {@code codec} will be used for
	 * encoding and decoding of data.
	 *
	 * @param env        The configuration environment
	 * @param dispatcher The dispatcher used to send events
	 * @param connectAddress The root host and port to connect relatively from in http handlers
	 * @param options    The configuration options for the client's socket
	 * @param sslOptions The SSL configuration options for the client's socket
	 * @param codec      The codec used to encode and decode data
	 */
	public NettyHttpClient(final Environment env,
	                       final Dispatcher dispatcher,
	                       final InetSocketAddress connectAddress,
	                       final ClientSocketOptions options,
	                       final SslOptions sslOptions,
	                       final Codec<Buffer, IN, OUT> codec) {
		super(env, dispatcher, codec);

		this.client = new NettyTcpClient<IN, OUT>(
				env,
				dispatcher,
				connectAddress,
				options,
				sslOptions,
				codec
		) {
			@Override
			protected NettyChannelStream<IN, OUT> bindChannel(Object nativeChannel, long prefetch) {
				NettyHttpClient.this.bindChannel(nativeChannel, prefetch);
				return null;
			}

			@Override
			public InetSocketAddress getConnectAddress() {
				if(connectAddress != null) return connectAddress;
				try {
					URL url = new URL(lastURL);
					String host = url.getHost();
					int port = url.getPort();
					return new InetSocketAddress(host, port);
				} catch (Exception e) {
					throw new IllegalArgumentException(e);
				}
			}
		};
	}

	@Override
	public Promise<HttpChannel<IN, OUT>> request(final Method method, final String url,
	                                            final Function<HttpChannel<IN, OUT>, ? extends Publisher<? extends OUT>>
			                                            handler) {
		lastURL = url;
		Assert.isTrue(method != null && url != null);

		return observe(new Consumer<HttpChannel<IN, OUT>>() {
			@Override
			public void accept(HttpChannel<IN, OUT> inoutHttpChannel) {
				((NettyHttpClientChannel) inoutHttpChannel)
						.getNettyRequest()
						.setUri(URI.create(url).getPath())
						.setMethod(new HttpMethod(method.getName()));

				if(handler != null) {
					addWritePublisher(handler.apply(inoutHttpChannel));
				}
			}
		}).next();
	}

	@Override
	public Promise<Boolean> open() {
		return client.open();
	}

	@Override
	public Stream<Boolean> open(final Reconnect reconnect) {
		return client.open(reconnect);
	}

	@Override
	public Promise<Boolean> close() {
		return client.close();
	}

	protected NettyHttpChannel<IN, OUT> createClientRequest(final NettyChannelStream<IN, OUT> tcpStream, final
	HttpRequest
			request) {

		NettyHttpChannel<IN, OUT> httpChannel = new NettyHttpClientChannel(tcpStream, request);

		notifyNewChannel(httpChannel);
		mergeWrite(httpChannel);
		return httpChannel;
	}

	/*@Override
	protected Consumer<Void> completeConsumer(final HttpChannel<IN, OUT> ch) {
		return new Consumer<Void>() {
			@Override
			@SuppressWarnings("unchecked")
			public void accept(Void aVoid) {
				((NettyHttpChannel)ch).write(new DefaultLastHttpContent(Unpooled.EMPTY_BUFFER), null, true);
			}
		};
	}*/

	@Override
	protected HttpChannel<IN, OUT> bindChannel(Object nativeChannel, long prefetch) {
		SocketChannel ch = (SocketChannel) nativeChannel;
		int backlog = 128;

		NettyChannelStream<IN, OUT> netChannel = new NettyChannelStream<IN, OUT>(
				getEnvironment(),
				getDefaultCodec(),
				prefetch == -1l ? getPrefetchSize() : prefetch,
				client,
				new NettyEventLoopDispatcher(ch.eventLoop(), backlog),
				getDispatcher(),
				ch
		);


		ChannelPipeline pipeline = ch.pipeline();
		if (log.isDebugEnabled()) {
			pipeline.addLast(new LoggingHandler(NettyHttpClient.class));
		}
		pipeline
				.addLast(new HttpClientCodec())
				.addLast(new NettyHttpClientHandler<IN, OUT>(netChannel, this));
		return null;
	}

	private class NettyHttpClientChannel extends NettyHttpChannel<IN, OUT> {


		final         Buffer                      body;
		private final NettyChannelStream<IN, OUT> tcpStream;
		private final HttpRequest                 request;

		public NettyHttpClientChannel(NettyChannelStream<IN, OUT> tcpStream, HttpRequest request) {
			super(tcpStream, NettyHttpClient.this.client, request, NettyHttpClient.this.getDefaultCodec());
			this.tcpStream = tcpStream;
			this.request = request;
			body = new Buffer();
		}

		@Override
		protected void write(ByteBuffer data, Subscriber<?> onComplete, boolean flush) {
			body.append(data);
			if (flush) {
				write(1, null, true);
			}
		}

		@Override
		protected void write(Object data, Subscriber<?> onComplete, boolean flush) {
			if (HEADERS_SENT.compareAndSet(this, 0, 1)) {
				HttpRequest req = new DefaultFullHttpRequest(
						request.getProtocolVersion(),
						request.getMethod(),
						request.getUri(),
						Unpooled.wrappedBuffer(body.flip().byteBuffer()));
				HttpHeaders.setContentLength(req, body.limit());
				HttpHeaders.setHeader(req, HttpHeaders.Names.CONTENT_TYPE,
						HttpHeaders.getHeader(request, HttpHeaders.Names.CONTENT_TYPE));
				tcpStream.write(req, null, true);
			}
		}
	}
}
