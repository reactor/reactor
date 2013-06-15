/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

package reactor.tcp.netty;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;

import java.net.SocketAddress;

/**
 * A simple {@link ChannelHandlerAdapter} that logs when a server is bound, a client is connected, or an exception is
 * thrown.
 *
 * @author Jon Brisbin
 */
public class LoggingHandler extends ChannelHandlerAdapter {

	private final Logger log;

	public LoggingHandler(Logger log) {
		this.log = log;
	}

	@Override
	public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelFuture future) throws Exception {
		if (log.isInfoEnabled()) {
			log.info("BIND {}", localAddress);
		}
		super.bind(ctx, localAddress, future);
	}

	@Override
	public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelFuture future) throws Exception {
		if (log.isDebugEnabled()) {
			log.debug("CONNECT {}", remoteAddress);
		}
		super.connect(ctx, remoteAddress, localAddress, future);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (log.isErrorEnabled()) {
			log.error(cause.getMessage(), cause);
		}
	}

}
