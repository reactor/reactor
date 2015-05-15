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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.*;
import reactor.io.buffer.Buffer;
import reactor.io.codec.StringCodec;
import reactor.io.net.http.model.Status;

/**
 * Conversion between Netty types  and Reactor types ({@link NettyHttpChannel} and {@link Buffer}).
 *
 * @author Stephane Maldini
 */
public class NettyHttpWSServerHandler<IN, OUT> extends NettyHttpServerHandler<IN, OUT> {

	private final WebSocketServerHandshaker handshaker;

	private final boolean plainText;

	public NettyHttpWSServerHandler(String wsUrl, String protocols, NettyHttpServerHandler<IN, OUT> originalHandler) {
		super(originalHandler.getHandler(), originalHandler.getChannelStream());
		this.request = originalHandler.request;

		this.plainText = originalHandler.getChannelStream().getEncoder() instanceof StringCodec.StringEncoder;

		// Handshake
		WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(wsUrl, protocols, true);
		handshaker = wsFactory.newHandshaker(request.getNettyRequest());
		if (handshaker == null) {
			WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(channelStream.delegate());
		} else {
			handshaker.handshake(channelStream.delegate(), request.getNettyRequest());
		}
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object frame) throws Exception {
		if (CloseWebSocketFrame.class.equals(frame.getClass())) {
			handshaker.close(ctx.channel(), ((CloseWebSocketFrame) frame).retain());
			if(channelSubscription != null) {
				channelSubscription.onComplete();
				channelSubscription = null;
			}
			return;
		}
		if (PingWebSocketFrame.class.isAssignableFrom(frame.getClass())) {
			ctx.channel().write(new PongWebSocketFrame(((PingWebSocketFrame)frame).content().retain()));
			return;
		}
		doRead(ctx, ((WebSocketFrame)frame).content());
	}

	protected void writeLast(ChannelHandlerContext ctx){
		ChannelFuture f = ctx.channel().writeAndFlush(new CloseWebSocketFrame());
	if (!request.isKeepAlive() || request.responseStatus() != Status.OK) {
			f.addListener(ChannelFutureListener.CLOSE);
		}
	}

	@Override
	protected ChannelFuture writeFirst(ChannelHandlerContext ctx) {
		return ctx.newSucceededFuture();
	}

	@Override
	protected ChannelFuture doOnWrite(final Object data, final ChannelHandlerContext ctx) {
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
}
