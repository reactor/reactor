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

package reactor.io.net.impl.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.Environment;
import reactor.core.Dispatcher;
import reactor.core.processor.CancelException;
import reactor.core.support.Exceptions;
import reactor.fn.Consumer;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.ChannelStream;
import reactor.io.net.PeerStream;
import reactor.rx.action.Control;
import reactor.rx.subscription.PushSubscription;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * {@link reactor.io.net.Channel} implementation that delegates to Netty.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NettyChannelStream<IN, OUT> extends ChannelStream<IN, OUT> {

	private final Channel ioChannel;

	public NettyChannelStream(@Nullable Environment env,
	                          @Nullable Codec<Buffer, IN, OUT> codec,
	                          long prefetch,
	                          @Nonnull PeerStream<IN, OUT, ChannelStream<IN, OUT>> peer,
	                          @Nonnull Dispatcher ioDispatcher,
	                          @Nonnull Dispatcher eventsDispatcher,
	                          @Nonnull Channel ioChannel) {
		super(env, codec, prefetch, peer, ioDispatcher, eventsDispatcher);
		this.ioChannel = ioChannel;
	}

	@Override
	public InetSocketAddress remoteAddress() {
		return (InetSocketAddress) ioChannel.remoteAddress();
	}


	@Override
	public Control sink(Publisher<? extends OUT> source) {
		final Control c = super.sink(source);
		ioChannel.closeFuture().addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				c.cancel();
			}
		});
		return c;
	}

	@Override
	public ConsumerSpec on() {
		return new NettyConsumerSpec();
	}

	@Override
	public Channel delegate() {
		return ioChannel;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected void doDecoded(IN in) {
		NettyNetChannelInboundHandler ch = ioChannel.pipeline().get(NettyNetChannelInboundHandler.class);
		PushSubscription<IN> subscription = ch == null ? null : ch.subscription();
		if (subscription != null) {
			try {
				subscription.onNext(in);
			} catch (CancelException ce){
				//I
			}
		} else {
			super.doDecoded(in);
		}
	}

	@Override
	public void write(ByteBuffer data, Subscriber<?> onComplete, boolean flush) {
		ByteBuf buf = ioChannel.alloc().buffer(data.remaining());
		buf.writeBytes(data);
		write(buf, onComplete, flush);
	}

	@Override
	public void write(final Object data, final Subscriber<?> onComplete, final boolean flush) {
		ChannelFuture writeFuture = flush ? ioChannel.writeAndFlush(data) : ioChannel.write(data);

		writeFuture.addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				boolean success = future.isSuccess();

				if (!success) {
					Throwable t = Exceptions.addValueAsLastCause(future.cause(), data);
					if (null != onComplete) {
						onComplete.onError(t);
					}else if (Environment.alive()){
						Environment.get().routeError(t);
					}
				}else if (null != onComplete) {
					onComplete.onComplete();
				}
			}
		});
	}

	@Override
	public void flush() {
		if(ioChannel.isActive()) {
			ioChannel.flush();
		}
	}

	@Override
	public String toString() {
		return this.getClass().getName() + " {" +
				"channel=" + ioChannel +
				'}';
	}

	private class NettyConsumerSpec implements ConsumerSpec {
		@Override
		public ConsumerSpec close(final Consumer<Void> onClose) {
			ioChannel.pipeline().addLast(new ChannelDuplexHandler() {
				@Override
				public void channelInactive(ChannelHandlerContext ctx) throws Exception {
					onClose.accept(null);
					super.channelInactive(ctx);
				}
			});
			return this;
		}

		@Override
		public ConsumerSpec readIdle(long idleTimeout, final Consumer<Void> onReadIdle) {
			ioChannel.pipeline().addFirst(new IdleStateHandler(idleTimeout, 0, 0, TimeUnit.MILLISECONDS) {
				@Override
				protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
					if (evt.state() == IdleState.READER_IDLE) {
						onReadIdle.accept(null);
					}
					super.channelIdle(ctx, evt);
				}
			});
			return this;
		}

		@Override
		public ConsumerSpec writeIdle(long idleTimeout, final Consumer<Void> onWriteIdle) {
			ioChannel.pipeline().addLast(new IdleStateHandler(0, idleTimeout, 0, TimeUnit.MILLISECONDS) {
				@Override
				protected void channelIdle(ChannelHandlerContext ctx, IdleStateEvent evt) throws Exception {
					if (evt.state() == IdleState.WRITER_IDLE) {
						onWriteIdle.accept(null);
					}
					super.channelIdle(ctx, evt);
				}
			});
			return this;
		}
	}

}
