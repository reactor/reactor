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

package reactor.io.netty.impl.netty.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.ReferenceCountUtil;
import reactor.io.buffer.Buffer;
import reactor.io.codec.StringCodec;
import reactor.io.netty.ChannelStream;
import reactor.io.netty.ReactorChannelHandler;
import reactor.io.netty.impl.netty.NettyChannelStream;

/**
 * @author Stephane Maldini
 */
public class NettyHttpWSClientHandler<IN, OUT> extends NettyHttpClientHandler<IN, OUT> {

	private final WebSocketClientHandshaker handshaker;

	private final boolean plainText;

	public NettyHttpWSClientHandler(
			ReactorChannelHandler<IN, OUT, ChannelStream<IN, OUT>> handler,
			NettyChannelStream<IN, OUT> tcpStream,
			WebSocketClientHandshaker handshaker) {
		super(handler, tcpStream);
		this.handshaker = handshaker;

		this.plainText = tcpStream.getEncoder() instanceof StringCodec.StringEncoder;
	}

	@Override
	protected ChannelFuture writeFirst(ChannelHandlerContext ctx) {
		return ctx.newSucceededFuture();
	}

	@Override
	public void channelActive(final ChannelHandlerContext ctx) throws Exception {
		handshaker.handshake(ctx.channel()).addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				ctx.read();
			}
		});
	}

	@Override
	protected void writeLast(ChannelHandlerContext ctx) {
		ctx.writeAndFlush(new CloseWebSocketFrame());
	}

	@Override
	protected void postRead(ChannelHandlerContext ctx, Object msg) {
		//IGNORE
	}

	@Override
	@SuppressWarnings("unchecked")
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		Class<?> messageClass = msg.getClass();
		if (!handshaker.isHandshakeComplete()) {
			handshaker.finishHandshake(ctx.channel(), (FullHttpResponse) msg);
			NettyHttpWSClientHandler.super.channelActive(ctx);
			super.channelRead(ctx, msg);
			return;
		}

		if (TextWebSocketFrame.class.isAssignableFrom(messageClass)) {
			try {
				Buffer buffer = Buffer.wrap(((TextWebSocketFrame) msg).text());
				if (channelStream.getDecoder() == null) {
					channelSubscription.onNext((IN) buffer);
				} else {
					IN d = channelStream.getDecoder().apply(buffer);
					if (d != null) {
						channelSubscription.onNext(d);
					}
				}
			} finally {
				ReferenceCountUtil.release(msg);
			}
		} else if (CloseWebSocketFrame.class.isAssignableFrom(messageClass)) {
			ctx.close();
		} else {
			doRead(ctx, ((WebSocketFrame)msg).content());
		}
	}

	@Override
	protected ChannelFuture doOnWrite(Object data, ChannelHandlerContext ctx) {
		if (data.getClass().equals(Buffer.class)) {
			if(!plainText) {
				return ctx.write(new BinaryWebSocketFrame(convertBufferToByteBuff(ctx, (Buffer) data)));
			}else{
				return ctx.write(new TextWebSocketFrame(convertBufferToByteBuff(ctx, (Buffer) data)));
			}
		} else {
			return ctx.write(data);
		}
	}

	@Override
	protected void doOnTerminate(ChannelHandlerContext ctx, ChannelFuture last, final ChannelPromise promise) {
		if (ctx.channel().isOpen()) {
			ChannelFutureListener listener = new ChannelFutureListener() {
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if (future.isSuccess()) {
						promise.trySuccess();
					} else {
						promise.tryFailure(future.cause());
					}
				}
			};

			if (last != null) {
				ctx.flush();
				last.addListener(listener);
			} else {
				ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(listener);
			}
		} else {
			promise.trySuccess();
		}
	}
}
