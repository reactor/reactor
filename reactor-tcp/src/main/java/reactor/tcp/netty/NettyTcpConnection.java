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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.socket.SocketChannel;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.io.Buffer;
import reactor.tcp.AbstractTcpConnection;
import reactor.tcp.encoding.Codec;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * A {@link reactor.tcp.TcpConnection} implementation that uses Netty.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class NettyTcpConnection<IN, OUT> extends AbstractTcpConnection<IN, OUT> {

	private volatile SocketChannel     channel;
	private volatile InetSocketAddress remoteAddress;

	NettyTcpConnection(final Environment env,
										 Codec<Buffer, IN, OUT> codec,
										 Dispatcher ioDispatcher,
										 Reactor eventsReactor,
										 SocketChannel channel) {
		this(env, codec, ioDispatcher, eventsReactor, channel, null);
	}

	NettyTcpConnection(final Environment env,
										 Codec<Buffer, IN, OUT> codec,
										 Dispatcher ioDispatcher,
										 Reactor eventsReactor,
										 SocketChannel channel,
										 InetSocketAddress remoteAddress) {
		super(env, codec, ioDispatcher, eventsReactor);
		this.channel = channel;
		this.remoteAddress = remoteAddress;
	}

	void reconnected(SocketChannel channel, InetSocketAddress remoteAddress) {
		this.channel = channel;
		this.remoteAddress = remoteAddress;
	}

	@Override
	public void close() {
		super.close();
		try {
			channel.close().await();
		} catch (InterruptedException e) {
			throw new IllegalStateException(e.getMessage(), e);
		}
	}

	@Override
	public boolean consumable() {
		return !channel.isInputShutdown();
	}

	@Override
	public boolean writable() {
		return !channel.isOutputShutdown();
	}

	@Override
	public InetSocketAddress remoteAddress() {
		return remoteAddress;
	}

	@Override
	protected void write(Buffer data, final Consumer<Boolean> onComplete) {
		write(data.byteBuffer(), onComplete);
	}

	protected void write(ByteBuffer data, final Consumer<Boolean> onComplete) {
		ByteBuf buf = channel.alloc().buffer(data.remaining());
		buf.writeBytes(data);

		write(buf, onComplete);
	}

	@Override
	protected void write(Object data, final Consumer<Boolean> onComplete) {
		ChannelFuture writeFuture = channel.writeAndFlush(data);
		writeFuture.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				boolean success = future.isSuccess();

				if (!success) {
					Throwable t = future.cause();
					eventsReactor.notify(t, Event.wrap(t));
				}

				if (null != onComplete) {
					onComplete.accept(success);
				}
			}
		});
	}

	@Override
	public String toString() {
		return "NettyTcpConnection{" +
				"channel=" + channel +
				", remoteAddress=" + remoteAddress +
				'}';
	}

}
