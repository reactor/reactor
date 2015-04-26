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

import io.netty.buffer.ByteBufHolder;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpContent;
import org.reactivestreams.Subscription;
import reactor.io.buffer.Buffer;
import reactor.io.net.ChannelStream;
import reactor.io.net.ReactorChannelHandler;
import reactor.io.net.impl.netty.NettyChannelHandlerBridge;
import reactor.io.net.impl.netty.NettyChannelStream;
import reactor.rx.action.support.DefaultSubscriber;

/**
 * Conversion between Netty types ({@link io.netty.handler.codec.http.HttpRequest}, {@link io.netty.handler.codec
 * .http.HttpResponse}, {@link io.netty.handler.codec.http.HttpContent} and {@link io.netty.handler.codec.http
 * .LastHttpContent})
 * and Reactor types ({@link NettyHttpChannel} and {@link reactor.io.buffer.Buffer}).
 *
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public class NettyHttpServerHandler<IN, OUT> extends NettyChannelHandlerBridge<IN, OUT> {

	private final NettyChannelStream<IN, OUT> tcpStream;
	private       NettyHttpChannel<IN, OUT>   request;

	public NettyHttpServerHandler(
			ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler,
			NettyChannelStream<IN, OUT> tcpStream) {
		super(handler, tcpStream);
		this.tcpStream = tcpStream;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		ctx.fireChannelActive();
		if (ctx.channel().isActive()) {
			ctx.read();
		}
	}

	@Override
	public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
		Class<?> messageClass = msg.getClass();
		if (request == null && io.netty.handler.codec.http.HttpRequest.class.isAssignableFrom(messageClass)) {
			request = new NettyHttpChannel<IN, OUT>(tcpStream, (io.netty.handler.codec.http.HttpRequest) msg);

			handler.apply(request)
					.subscribe(new DefaultSubscriber<Void>() {
						@Override
						public void onSubscribe(Subscription s) {
							ctx.writeAndFlush(request.getNettyResponse());
							s.request(Long.MAX_VALUE);
						}

						@Override
						public void onError(Throwable t) {
							log.error("Error processing connection. Closing the channel.", t);
							if(channelSubscription == null) {
								ctx.channel().close();
							}
						}

						@Override
						public void onComplete() {
							if(channelSubscription == null) {
								ctx.channel().close();
							}
						}
					});

		} if (HttpContent.class.isAssignableFrom(messageClass)) {
			super.channelRead(ctx, ((ByteBufHolder) msg).content());

			if(DefaultLastHttpContent.class.equals(msg.getClass())) {
				if (channelSubscription != null) {
					channelSubscription.onComplete();
					channelSubscription = null;
				}
			}
		}
	}

	@Override
	protected ChannelFuture doOnWrite(Object data, ChannelHandlerContext ctx) {
		if (data.getClass().equals(Buffer.class)) {
			return ctx.write(new DefaultHttpContent(Unpooled.wrappedBuffer(((Buffer) data).byteBuffer())));
		} else {
			return ctx.write(data);
		}
	}
}
