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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.util.AttributeKey;
import reactor.io.net.impl.netty.NettyChannelStream;
import reactor.io.net.impl.netty.NettyNetChannelInboundHandler;

/**
 * @author Stephane Maldini
 */
public class NettyHttpClientHandler<IN, OUT> extends NettyNetChannelInboundHandler<IN> {

	private static final AttributeKey<NettyHttpChannel> KEY = AttributeKey.valueOf("httpChannel");
	private final NettyChannelStream<IN, OUT> tcpStream;
	private final NettyHttpClient<IN, OUT>    client;

	public NettyHttpClientHandler(NettyChannelStream<IN, OUT> tcpStream,
	                              NettyHttpClient<IN, OUT> client) {
		super(tcpStream.in(), tcpStream);
		this.client = client;
		this.tcpStream = tcpStream;
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		super.channelActive(ctx);

		NettyHttpChannel<IN, OUT> inoutHttpChannel =
				client.createClientRequest(tcpStream,
						new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
				);
		ctx.attr(KEY).set(inoutHttpChannel);
		tcpStream.subscribe(inoutHttpChannel.in());
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		Class<?> messageClass = msg.getClass();
		if (HttpResponse.class.isAssignableFrom(messageClass)) {
			NettyHttpChannel httpChannel = ctx.attr(KEY).getAndRemove();
			if(httpChannel != null){
				httpChannel.setNettyResponse((HttpResponse) msg);
			}
		} else if (HttpContent.class.isAssignableFrom(messageClass)) {
			super.channelRead(ctx, ((ByteBufHolder) msg).content());
		}
	}

}
