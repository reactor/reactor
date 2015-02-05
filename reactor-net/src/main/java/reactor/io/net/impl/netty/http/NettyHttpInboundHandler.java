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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import reactor.io.buffer.Buffer;
import reactor.io.net.http.ServerRequest;
import reactor.io.net.impl.netty.NettyChannelStream;
import reactor.io.net.impl.netty.NettyNetChannelInboundHandler;

/**
 * Conversion between Netty types ({@link io.netty.handler.codec.http.HttpRequest}, {@link io.netty.handler.codec
 * .http.HttpResponse}, {@link io.netty.handler.codec.http.HttpContent} and {@link io.netty.handler.codec.http
 * .LastHttpContent})
 * and Reactor types ({@link NettyServerRequest} and {@link reactor.io.buffer.Buffer}).
 *
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public class NettyHttpInboundHandler<IN, OUT> extends NettyNetChannelInboundHandler<IN> {

	private final NettyHttpServer<IN, OUT>     server;
	private final NettyChannelStream<IN, OUT> tcpStream;
	private       ServerRequest<IN, OUT>      request;

	public NettyHttpInboundHandler(NettyChannelStream<IN, OUT> tcpStream,
	                               NettyHttpServer<IN, OUT> server) {
		super(tcpStream.in(), tcpStream);
		this.server = server;
		this.tcpStream = tcpStream;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		super.channelActive(ctx);
		if(ctx.channel().isActive()){
			ctx.read();
		}
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		Class<?> messageClass = msg.getClass();
		if (request == null && HttpRequest.class.isAssignableFrom(messageClass)) {
			request = server.createServerRequest(tcpStream, (HttpRequest)msg);
		} else if (HttpContent.class.isAssignableFrom(messageClass)) {
			ByteBuf content = ((ByteBufHolder) msg).content();
			super.channelRead(ctx, new Buffer(content.nioBuffer()));
			if (LastHttpContent.class.isAssignableFrom(messageClass)) {
				super.channelInactive(ctx);
			}
		}
	}


	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		super.channelReadComplete(ctx);
		ctx.pipeline().flush(); // If there is nothing to flush, this is a short-circuit in netty.
	}

}
