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
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.*;
import org.reactivestreams.Subscription;
import reactor.io.buffer.Buffer;
import reactor.io.net.ChannelStream;
import reactor.io.net.ReactorChannelHandler;
import reactor.io.net.impl.netty.NettyChannelHandlerBridge;
import reactor.io.net.impl.netty.NettyChannelStream;
import reactor.rx.action.support.DefaultSubscriber;

import java.nio.ByteBuffer;

/**
 * @author Stephane Maldini
 */
public class NettyHttpClientHandler<IN, OUT> extends NettyChannelHandlerBridge<IN, OUT> {

	private final NettyChannelStream<IN, OUT> tcpStream;
	private final Buffer                      body;
	private       NettyHttpChannel<IN, OUT>   request;

	public NettyHttpClientHandler(
			ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler,
			NettyChannelStream<IN, OUT> tcpStream) {
		super(handler, tcpStream);
		this.tcpStream = tcpStream;
		this.body = new Buffer();
	}

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		ctx.fireChannelActive();

		request =
				new NettyHttpChannel<>(tcpStream,
						new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
				);

		handler.apply(request)
				.subscribe(new DefaultSubscriber<Void>() {
					@Override
					public void onSubscribe(Subscription s) {
						ctx.writeAndFlush(request.getNettyRequest());
						s.request(Long.MAX_VALUE);
					}

					@Override
					public void onError(Throwable t) {
						log.error("Error processing connection. Closing the channel.", t);
						ctx.channel().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
					}

					@Override
					public void onComplete() {
						ctx.channel().writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
					}
				});
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		Class<?> messageClass = msg.getClass();
		if (HttpResponse.class.isAssignableFrom(messageClass)) {
			if (request != null) {
				request.setNettyResponse((HttpResponse) msg);
			}
		} else if (HttpContent.class.isAssignableFrom(messageClass)) {
			super.channelRead(ctx, ((ByteBufHolder) msg).content());
		}
	}

	@Override
	protected ChannelFuture doOnWrite(Object data, ChannelHandlerContext ctx) {
		if (data.getClass().equals(Buffer.class)) {
			body.append((Buffer) data);
			return null;
		} else {
			return ctx.write(data);
		}
	}

	@Override
	protected void doOnTerminate(ChannelHandlerContext ctx, ChannelFuture last, final ChannelPromise promise) {
		ByteBuffer byteBuffer = body.flip().byteBuffer();
		HttpRequest req = new DefaultFullHttpRequest(
				request.getNettyRequest().getProtocolVersion(),
				request.getNettyRequest().getMethod(),
				request.getNettyRequest().getUri(),
				byteBuffer != null ? Unpooled.wrappedBuffer(byteBuffer) : Unpooled.EMPTY_BUFFER);

		if(byteBuffer != null){
			HttpHeaders.setContentLength(req, body.limit());

			String header = HttpHeaders.getHeader(request.getNettyRequest(), HttpHeaders.Names.CONTENT_TYPE);
			if (header != null) {
				HttpHeaders.setHeader(req, HttpHeaders.Names.CONTENT_TYPE, header);
			}
		}


		ctx.writeAndFlush(req).addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (future.isSuccess()) {
					promise.trySuccess();
				} else {
					promise.tryFailure(future.cause());
				}
			}
		});
	}
}
