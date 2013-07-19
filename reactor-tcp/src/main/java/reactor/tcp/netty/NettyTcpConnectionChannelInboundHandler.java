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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import reactor.io.Buffer;

/**
 * A {@link ChannelInboundHandlerAdapter} implementation that uses NettyTcpConnection.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */

class NettyTcpConnectionChannelInboundHandler extends ChannelInboundHandlerAdapter {

	private final NettyTcpConnection<?, ?> conn;

	private ByteBuf remainder;

	NettyTcpConnectionChannelInboundHandler(NettyTcpConnection<?, ?> conn) {
		this.conn = conn;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object m) throws Exception {
		if (m instanceof ByteBuf) {
			ByteBuf data = (ByteBuf) m;

			if (remainder == null) {
				try {
					passToConnection(data);
				} finally {
					if (data.isReadable()) {
						remainder = data;
					} else {
						data.release();
					}
				}
			} else {
				if (!bufferHasSufficientCapacity(remainder, data)) {
					ByteBuf combined = createCombinedBuffer(remainder, data, ctx);
					remainder.release();
					remainder = combined;
				} else {
					remainder.writeBytes(data);
				}
				data.release();

				try {
					passToConnection(remainder);
				} finally {
					if (remainder.isReadable()) {
						remainder.discardSomeReadBytes();
					} else {
						remainder.release();
						remainder = null;
					}
				}
			}
		}
	}

	private boolean bufferHasSufficientCapacity(ByteBuf receiver, ByteBuf provider) {
		return receiver.writerIndex() <= receiver.maxCapacity() - provider.readableBytes();
	}

	private ByteBuf createCombinedBuffer(ByteBuf partOne, ByteBuf partTwo, ChannelHandlerContext ctx) {
    ByteBuf combined = ctx.alloc().buffer(partOne.readableBytes() + partTwo.readableBytes());
    combined.writeBytes(partOne);
    combined.writeBytes(partTwo);
    return combined;
	}

	private void passToConnection(ByteBuf data) {
		Buffer b = new Buffer(data.nioBuffer());
		int start = b.position();
		conn.read(b);
		data.skipBytes(b.position() - start);
	}
}
