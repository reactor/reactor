/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.net.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.event.Event;
import reactor.event.dispatch.Dispatcher;
import reactor.function.Consumer;
import reactor.io.Buffer;
import reactor.io.encoding.Codec;
import reactor.net.AbstractNetChannel;
import reactor.rx.Promise;
import reactor.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * {@link reactor.net.NetChannel} implementation that delegates to Netty.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyNetChannel<IN, OUT> extends AbstractNetChannel<IN, OUT> {

	private final Channel ioChannel;

	private volatile boolean closing = false;

	public NettyNetChannel(@Nonnull Environment env,
	                       @Nullable Codec<Buffer, IN, OUT> codec,
	                       @Nonnull Dispatcher ioDispatcher,
	                       @Nonnull Reactor eventsReactor,
	                       @Nonnull Channel ioChannel) {
		super(env, codec, ioDispatcher, eventsReactor);
		this.ioChannel = ioChannel;
	}

	public boolean isClosing() {
		return closing;
	}

	@Override
	public InetSocketAddress remoteAddress() {
		return (InetSocketAddress) ioChannel.remoteAddress();
	}

	@Override
	public void close(@Nullable final Consumer<Boolean> onClose) {
		if (closing) {
			return;
		}
		closing = true;
		ioChannel.close().addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (null != onClose) {
					getEventsReactor().schedule(onClose, future.isSuccess());
				} else if (!future.isSuccess()) {
					log.error(future.cause().getMessage(), future.cause());
				}
				closing = false;
			}
		});
	}

	@Override
	public ConsumerSpec on() {
		return new NettyConsumerSpec();
	}

	@Override
	protected void write(ByteBuffer data, Promise<Void> onComplete, boolean flush) {
		ByteBuf buf = ioChannel.alloc().buffer(data.remaining());
		buf.writeBytes(data);
		write(buf, onComplete, flush);
	}

	@Override
	protected void write(Object data, final Promise<Void> onComplete, boolean flush) {
		ChannelFuture writeFuture = ioChannel.write(Tuple.of(data, flush));
		writeFuture.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				boolean success = future.isSuccess();

				if (!success) {
					Throwable t = future.cause();
					getEventsReactor().notify(t.getClass(), Event.wrap(t));
				}
				if (null != onComplete) {
					onComplete.onComplete();
				}
			}
		});
	}

	@Override
	protected void flush() {
		ioChannel.write(Tuple.of(null, true));
	}

	@Override
	public String toString() {
		return "NettyNetChannel{" +
				"channel=" + ioChannel +
				'}';
	}

	private class NettyConsumerSpec implements ConsumerSpec {
		@Override
		public ConsumerSpec close(final Runnable onClose) {
			ioChannel.pipeline().addLast(new ChannelDuplexHandler() {
				@Override
				public void channelInactive(ChannelHandlerContext ctx) throws Exception {
					onClose.run();
					super.channelInactive(ctx);
				}
			});
			return this;
		}

		@Override
		public ConsumerSpec readIdle(long idleTimeout, final Runnable onReadIdle) {
			ioChannel.pipeline().addFirst(new IdleStateHandler(idleTimeout, 0, 0, TimeUnit.MILLISECONDS) {
				@Override
				protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
					if (evt.state() == IdleState.READER_IDLE) {
						onReadIdle.run();
					}
					super.channelIdle(ctx, evt);
				}
			});
			return this;
		}

		@Override
		public ConsumerSpec writeIdle(long idleTimeout, final Runnable onWriteIdle) {
			ioChannel.pipeline().addLast(new IdleStateHandler(0, idleTimeout, 0, TimeUnit.MILLISECONDS) {
				@Override
				protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
					if (evt.state() == IdleState.WRITER_IDLE) {
						onWriteIdle.run();
					}
					super.channelIdle(ctx, evt);
				}
			});
			return this;
		}
	}

}
