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
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.logging.LoggingHandler;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Publishers;
import reactor.core.subscriber.SubscriberWithContext;
import reactor.core.support.Assert;
import reactor.fn.BiConsumer;
import reactor.fn.Consumer;
import reactor.fn.timer.Timer;
import reactor.fn.tuple.Tuple2;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.ChannelStream;
import reactor.io.net.ReactorChannelHandler;
import reactor.io.net.Reconnect;
import reactor.io.net.config.ClientSocketOptions;
import reactor.io.net.config.SslOptions;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.HttpClient;
import reactor.io.net.http.model.Method;
import reactor.io.net.impl.netty.NettyChannelStream;
import reactor.io.net.impl.netty.tcp.NettyTcpClient;
import reactor.rx.Promise;
import reactor.rx.Promises;
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * A Netty-based {@code TcpClient}.
 *
 * @param <IN>  The type that will be received by this client
 * @param <OUT> The type that will be sent by this client
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyHttpClient<IN, OUT> extends HttpClient<IN, OUT> {

	private final static Logger log = LoggerFactory.getLogger(NettyHttpClient.class);

	private final NettyTcpClient<IN, OUT>            client;

	private URI lastURI = null;

	/**
	 * Creates a new NettyTcpClient that will use the given {@code env} for configuration and the given {@code
	 * reactor} to
	 * send events. The number of IO threads used by the client is configured by the environment's {@code
	 * reactor.tcp.ioThreadCount} property. In its absence the number of IO threads will be equal to the {@link
	 * reactor.Processors#DEFAULT_POOL_SIZE number of available processors}. </p> The client will connect to the given {@code
	 * connectAddress}, configuring its socket using the given {@code opts}. The given {@code codec} will be used for
	 * encoding and decoding of data.
	 *
	 * @param timer     The default timer configured
	 * @param connectAddress The root host and port to connect relatively from in http handlers
	 * @param options        The configuration options for the client's socket
	 * @param sslOptions     The SSL configuration options for the client's socket
	 * @param codec          The codec used to encode and decode data
	 */
	public NettyHttpClient(final Timer timer,
	                       final InetSocketAddress connectAddress,
	                       final ClientSocketOptions options,
	                       final SslOptions sslOptions,
	                       final Codec<Buffer, IN, OUT> codec) {
		super(timer, codec, options);

		this.client = new NettyTcpClient<IN, OUT>(
				timer,
				connectAddress,
				options,
				sslOptions,
				codec
		) {
			@Override
			protected void bindChannel(ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler, SocketChannel
					nativeChannel) {

				URI currentURI = lastURI;
				try {
					if (currentURI.getScheme() != null
							&& (currentURI.getScheme().toLowerCase().equals(HttpChannel.HTTPS_SCHEME) ||
							currentURI.getScheme().toLowerCase().equals(HttpChannel.WSS_SCHEME))) {
						addSecureHandler(nativeChannel);
					}
				} catch (Exception e) {
					nativeChannel.pipeline().fireExceptionCaught(e);
				}

				NettyHttpClient.this.bindChannel(handler, nativeChannel);
			}

			@Override
			public InetSocketAddress getConnectAddress() {
				if (connectAddress != null) return connectAddress;
				try {
					URI url = lastURI;
					String host = url != null && url.getHost() != null ? url.getHost() : "localhost";
					int port = url != null ? url.getPort() : -1;
					if (port == -1) {
						if (url != null && url.getScheme() != null && (
								url.getScheme().toLowerCase().equals(HttpChannel.HTTPS_SCHEME) ||
										url.getScheme().toLowerCase().equals(HttpChannel.WSS_SCHEME)
						)) {
							port = 443;
						} else {
							port = 80;
						}
					}
					return new InetSocketAddress(host, port);
				} catch (Exception e) {
					throw new IllegalArgumentException(e);
				}
			}
		};
	}

	@Override
	protected Promise<Void> doStart(final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>> handler) {
		return client.start(new ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>>() {
			@Override
			public Publisher<Void> apply(ChannelStream<IN, OUT> inoutChannelStream) {
				final NettyHttpChannel<IN, OUT> ch = ((NettyHttpChannel<IN, OUT>) inoutChannelStream);
				return handler.apply(ch);
			}
		});
	}

	@Override
	protected Stream<Tuple2<InetSocketAddress, Integer>> doStart(final ReactorChannelHandler<IN, OUT, HttpChannel<IN,
			OUT>> handler,
	                                                             final Reconnect reconnect) {
		return client.start(new ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>>() {
			@Override
			public Publisher<Void> apply(ChannelStream<IN, OUT> inoutChannelStream) {
				final NettyHttpChannel<IN, OUT> ch = ((NettyHttpChannel<IN, OUT>) inoutChannelStream);
				return handler.apply(ch);
			}
		}, reconnect);
	}

	@Override
	public Stream<? extends HttpChannel<IN, OUT>> request(final Method method, final String url,
	                                                       final ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>>
			                                                       handler) {
		final URI currentURI;
		try{
			Assert.isTrue(method != null && url != null);
			currentURI = parseURL(method, url);
			lastURI = currentURI;
		}catch(Exception e){
			return Streams.fail(e);
		}

		return Streams.wrap(Publishers.createWithDemand(new BiConsumer<Long, SubscriberWithContext<HttpChannel<IN, OUT>,  Void>>() {
			@Override
			public void accept(Long n, final SubscriberWithContext<HttpChannel<IN, OUT>, Void> subscriber) {
				if (started.get()) return;
				start(new ReactorChannelHandler<IN, OUT, HttpChannel<IN, OUT>>() {
					@Override
					public Publisher<Void> apply(HttpChannel<IN, OUT> inoutHttpChannel) {
						try {
							final NettyHttpChannel<IN, OUT> ch = ((NettyHttpChannel<IN, OUT>) inoutHttpChannel);
							ch.getNettyRequest()
							  .setUri(currentURI.getPath() + (currentURI.getQuery() == null ? "" : "?" + currentURI
								.getQuery()))
							  .setMethod(new HttpMethod(method.getName()))
							  .headers()
							  .add(HttpHeaders.Names.HOST, currentURI.getHost())
							  .add(HttpHeaders.Names.ACCEPT, "*/*");

							if (handler != null) {
								Publisher<Void> p = handler.apply(ch);
								subscriber.onNext(ch);
								subscriber.onComplete();
								return p;
							} else {
								ch.headers().removeTransferEncodingChunked();
								subscriber.onNext(ch);
								subscriber.onComplete();
								return ch.writeHeaders();
							}
						} catch (Throwable t) {
							subscriber.onError(t);
							return Promises.error(t);
						}
					}

				}).onError(new Consumer<Throwable>() {
					@Override
					public void accept(Throwable throwable) {
						subscriber.onError(throwable);
					}
				});
			}
		}));
	}

	private URI parseURL(Method method, String url) throws Exception{
		if(!url.startsWith(HttpChannel.HTTP_SCHEME) && !url.startsWith(HttpChannel.WS_SCHEME)){
			final String parsedUrl = (method.equals(Method.WS) ? HttpChannel.WS_SCHEME : HttpChannel.HTTP_SCHEME) + "://";
			if(url.startsWith("/")){
				return new URI(parsedUrl
						+ (lastURI != null && lastURI.getHost() != null ? lastURI.getHost() : "localhost")
						+ url);
			}else {
				return new URI(parsedUrl + url);
			}
		}else{
			return new URI(url);
		}
	}

	@Override
	protected final Promise<Void> doShutdown() {
		return client.shutdown();
	}

	protected void bindChannel(ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler, Object nativeChannel) {
		SocketChannel ch = (SocketChannel) nativeChannel;

		NettyChannelStream<IN, OUT> netChannel = new NettyChannelStream<IN, OUT>(
				getDefaultTimer(),
				getDefaultCodec(),
				getDefaultPrefetchSize(),
				ch
		);


		ChannelPipeline pipeline = ch.pipeline();
		if (log.isDebugEnabled()) {
			pipeline.addLast(new LoggingHandler(NettyHttpClient.class));
		}

		pipeline
				.addLast(new HttpClientCodec());

		URI currentURI = lastURI;
		if(currentURI.getScheme() != null &&
				currentURI.getScheme().toLowerCase().startsWith(HttpChannel.WS_SCHEME)){
			pipeline
					.addLast(new HttpObjectAggregator(8192))
					.addLast(
							new NettyHttpWSClientHandler<IN, OUT>(
									handler,
									netChannel,
									WebSocketClientHandshakerFactory.newHandshaker(
											lastURI, WebSocketVersion.V13, null, false, new DefaultHttpHeaders()))
					);
		}else {
			pipeline
					.addLast(new NettyHttpClientHandler<IN, OUT>(handler, netChannel));
		}
	}
}
